use crate::config::RiskConfig;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PaperBrokerStats {
    pub equity_brl: f64,
    pub equity_usdt: f64,
    pub realized_pnl_usdt: f64,
    pub open_positions: usize,
    pub trades_closed: u64,
    pub rejected_orders: u64,
    pub kill_switch: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PositionSide {
    Long,
    Short,
}

#[derive(Debug, Clone)]
pub struct PaperOrderRequest {
    pub side: PositionSide,
    pub entry_price: f64,
    pub stop_price: f64,
    pub target_price: f64,
    pub usdtbrl: f64,
    pub expected_slippage_usdt: f64,
    pub fee_rate: f64,
    pub exchange_max_leverage: f64,
    pub min_qty: f64,
    pub step_size: f64,
    pub tick_size: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RiskDecision {
    pub approved: bool,
    pub reason: String,
    pub qty: f64,
    pub notional_usdt: f64,
    pub leverage: f64,
    pub risk_usdt: f64,
    pub expected_fee_usdt: f64,
    pub expected_slippage_usdt: f64,
    pub liquidation_price: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperPosition {
    pub side: PositionSide,
    pub qty: f64,
    pub entry_price: f64,
    pub stop_price: f64,
    pub target_price: f64,
    pub notional_usdt: f64,
    pub leverage: f64,
    pub open_fee_usdt: f64,
}

#[derive(Debug, Clone)]
pub struct PaperBroker {
    config: RiskConfig,
    equity_brl: f64,
    usdtbrl: f64,
    realized_pnl_usdt: f64,
    open_positions: Vec<PaperPosition>,
    trades_closed: u64,
    rejected_orders: u64,
    kill_switch: bool,
}

impl PaperBroker {
    pub fn new(config: RiskConfig, usdtbrl: f64) -> Self {
        Self {
            equity_brl: config.starting_capital_brl,
            config,
            usdtbrl,
            realized_pnl_usdt: 0.0,
            open_positions: Vec::new(),
            trades_closed: 0,
            rejected_orders: 0,
            kill_switch: false,
        }
    }

    pub fn evaluate(&self, request: &PaperOrderRequest) -> RiskDecision {
        if self.kill_switch {
            return rejected("kill switch active");
        }
        if self.open_positions.len() >= self.config.max_concurrent_positions as usize {
            return rejected("max concurrent positions reached");
        }
        if request.usdtbrl <= f64::EPSILON {
            return rejected("invalid USDTBRL rate");
        }
        if request.entry_price <= 0.0 || request.stop_price <= 0.0 || request.target_price <= 0.0 {
            return rejected("invalid price");
        }

        let stop_distance = (request.entry_price - request.stop_price).abs();
        let target_distance = (request.target_price - request.entry_price).abs();
        if stop_distance <= f64::EPSILON || target_distance <= f64::EPSILON {
            return rejected("invalid stop or target distance");
        }

        if spread_or_slippage_ratio(request.expected_slippage_usdt, stop_distance)
            > self.config.max_slippage_to_stop_ratio
        {
            return rejected("expected slippage exceeds stop ratio");
        }

        let risk_usdt = self.config.risk_per_trade_brl / request.usdtbrl;
        let mut qty = risk_usdt / stop_distance;
        qty = round_step(qty, request.step_size);
        if qty < request.min_qty {
            return rejected("min qty would exceed allowed risk");
        }

        let notional_usdt = qty * request.entry_price;
        let expected_fee_usdt = notional_usdt * request.fee_rate * 2.0;
        if expected_fee_usdt / target_distance.max(1.0) > self.config.max_fee_to_target_ratio {
            return rejected("expected fee exceeds target ratio");
        }

        let equity_usdt = self.equity_brl / request.usdtbrl;
        let max_margin_notional = equity_usdt
            * self.config.max_margin_fraction_per_trade
            * self
                .config
                .leverage_paper_stress_cap
                .min(request.exchange_max_leverage);
        let max_cap_notional = equity_usdt * request.exchange_max_leverage;
        if notional_usdt > max_margin_notional.min(max_cap_notional) {
            return rejected("notional exceeds leverage or margin cap");
        }

        let gross_loss_pct = stop_distance / request.entry_price
            + request.fee_rate * 2.0
            + request.expected_slippage_usdt / request.entry_price
            + 0.005;
        let max_notional_by_risk = risk_usdt / gross_loss_pct.max(f64::EPSILON);
        let max_leverage_by_risk = max_notional_by_risk / equity_usdt.max(f64::EPSILON);
        let leverage = request
            .exchange_max_leverage
            .min(self.config.leverage_paper_stress_cap)
            .min(max_leverage_by_risk)
            .max(1.0);
        let liquidation_price = liquidation_price(request.side, request.entry_price, leverage);
        if liquidation_before_stop(request.side, liquidation_price, request.stop_price) {
            return rejected("liquidation before stop");
        }

        RiskDecision {
            approved: true,
            reason: "approved".to_string(),
            qty,
            notional_usdt,
            leverage,
            risk_usdt,
            expected_fee_usdt,
            expected_slippage_usdt: request.expected_slippage_usdt,
            liquidation_price: Some(liquidation_price),
        }
    }

    pub fn submit_market_order(&mut self, request: PaperOrderRequest) -> RiskDecision {
        let decision = self.evaluate(&request);
        if !decision.approved {
            self.rejected_orders += 1;
            return decision;
        }

        self.open_positions.push(PaperPosition {
            side: request.side,
            qty: decision.qty,
            entry_price: request.entry_price,
            stop_price: request.stop_price,
            target_price: request.target_price,
            notional_usdt: decision.notional_usdt,
            leverage: decision.leverage,
            open_fee_usdt: decision.expected_fee_usdt / 2.0,
        });
        decision
    }

    pub fn mark_price(&mut self, price: f64, fee_rate: f64) {
        let mut remaining = Vec::new();
        let mut closed = Vec::new();

        for position in self.open_positions.drain(..) {
            if position_hit_exit(&position, price) {
                closed.push(position);
            } else {
                remaining.push(position);
            }
        }

        for position in closed {
            let gross = match position.side {
                PositionSide::Long => (price - position.entry_price) * position.qty,
                PositionSide::Short => (position.entry_price - price) * position.qty,
            };
            let close_fee = position.qty * price * fee_rate;
            let pnl = gross - position.open_fee_usdt - close_fee;
            self.realized_pnl_usdt += pnl;
            self.equity_brl += pnl * self.usdtbrl;
            self.trades_closed += 1;
            if -self.realized_pnl_usdt * self.usdtbrl >= self.config.max_daily_loss_brl {
                self.kill_switch = true;
            }
        }

        self.open_positions = remaining;
    }

    pub fn stats(&self) -> PaperBrokerStats {
        PaperBrokerStats {
            equity_brl: self.equity_brl,
            equity_usdt: self.equity_brl / self.usdtbrl,
            realized_pnl_usdt: self.realized_pnl_usdt,
            open_positions: self.open_positions.len(),
            trades_closed: self.trades_closed,
            rejected_orders: self.rejected_orders,
            kill_switch: self.kill_switch,
        }
    }
}

fn rejected(reason: &str) -> RiskDecision {
    RiskDecision {
        approved: false,
        reason: reason.to_string(),
        qty: 0.0,
        notional_usdt: 0.0,
        leverage: 0.0,
        risk_usdt: 0.0,
        expected_fee_usdt: 0.0,
        expected_slippage_usdt: 0.0,
        liquidation_price: None,
    }
}

fn round_step(value: f64, step: f64) -> f64 {
    if step <= f64::EPSILON {
        return value;
    }
    (value / step).floor() * step
}

fn liquidation_price(side: PositionSide, entry: f64, leverage: f64) -> f64 {
    match side {
        PositionSide::Long => entry * (1.0 - 1.0 / leverage),
        PositionSide::Short => entry * (1.0 + 1.0 / leverage),
    }
}

fn liquidation_before_stop(side: PositionSide, liquidation: f64, stop: f64) -> bool {
    match side {
        PositionSide::Long => liquidation >= stop,
        PositionSide::Short => liquidation <= stop,
    }
}

fn position_hit_exit(position: &PaperPosition, price: f64) -> bool {
    match position.side {
        PositionSide::Long => price <= position.stop_price || price >= position.target_price,
        PositionSide::Short => price >= position.stop_price || price <= position.target_price,
    }
}

fn spread_or_slippage_ratio(value: f64, stop_distance: f64) -> f64 {
    value / stop_distance.max(f64::EPSILON)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn approves_and_sizes_order_from_risk() {
        let broker = PaperBroker::new(config(), 5.0);
        let decision = broker.evaluate(&request());

        assert!(decision.approved, "{}", decision.reason);
        assert_eq!(decision.risk_usdt, 1.0);
        assert_eq!(decision.qty, 0.1);
        assert!(decision.leverage >= 1.0);
    }

    #[test]
    fn rejects_when_min_qty_exceeds_risk() {
        let broker = PaperBroker::new(config(), 5.0);
        let mut request = request();
        request.min_qty = 1.0;

        let decision = broker.evaluate(&request);
        assert!(!decision.approved);
        assert_eq!(decision.reason, "min qty would exceed allowed risk");
    }

    #[test]
    fn closes_position_and_updates_ledger() {
        let mut broker = PaperBroker::new(config(), 5.0);
        let decision = broker.submit_market_order(request());
        assert!(decision.approved);
        broker.mark_price(120.0, 0.0004);

        let stats = broker.stats();
        assert_eq!(stats.open_positions, 0);
        assert_eq!(stats.trades_closed, 1);
        assert!(stats.equity_brl > 100.0);
    }

    #[test]
    fn daily_kill_switch_after_loss() {
        let mut cfg = config();
        cfg.max_daily_loss_brl = 0.1;
        let mut broker = PaperBroker::new(cfg, 5.0);
        broker.submit_market_order(request());
        broker.mark_price(90.0, 0.0004);

        assert!(broker.stats().kill_switch);
    }

    fn config() -> RiskConfig {
        RiskConfig {
            starting_capital_brl: 100.0,
            risk_per_trade_brl: 5.0,
            max_daily_loss_brl: 10.0,
            max_weekly_loss_brl: 20.0,
            max_concurrent_positions: 1,
            leverage_live_hard_cap_initial: 20.0,
            leverage_paper_stress_cap: 50.0,
            max_fee_to_target_ratio: 0.35,
            max_slippage_to_stop_ratio: 0.20,
            max_margin_fraction_per_trade: 0.20,
        }
    }

    fn request() -> PaperOrderRequest {
        PaperOrderRequest {
            side: PositionSide::Long,
            entry_price: 100.0,
            stop_price: 90.0,
            target_price: 120.0,
            usdtbrl: 5.0,
            expected_slippage_usdt: 1.0,
            fee_rate: 0.0004,
            exchange_max_leverage: 20.0,
            min_qty: 0.001,
            step_size: 0.001,
            tick_size: 0.1,
        }
    }
}
