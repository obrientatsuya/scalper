use crate::events::{
    AggressorSide, DepthDeltaEvent, Exchange, MarketEvent, TickerEvent, TradeEvent,
};
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, VecDeque},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const WINDOW_MS: i64 = 1_000;
const RECENT_EVENTS: usize = 32;
const STOP_POOL_HISTORY: usize = 256;
const ICEBERG_TRADE_MULTIPLE: f64 = 2.0;
const LARGE_PULL_QTY: f64 = 5.0;
const ROUND_NUMBER_SIZE: f64 = 100.0;

#[derive(Debug, Clone, Default, Serialize)]
pub struct HiddenLiquidityStats {
    pub events_seen: u64,
    pub hidden_liquidity_score: f64,
    pub iceberg_count: u64,
    pub spoof_pull_count: u64,
    pub liquidation_impulse_score: f64,
    pub crowded_positioning_score: f64,
    pub exchanges_seen: usize,
    pub cross_exchange_lead_lag: Option<LeadLagStats>,
    pub stop_pools: Vec<StopPoolStats>,
    pub recent_events: Vec<HiddenEventStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LeadLagStats {
    pub leader: String,
    pub lagger: String,
    pub lag_ms: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StopPoolStats {
    pub price: f64,
    pub touches: u64,
    pub last_touch_ms: i64,
    pub pool_score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HiddenEventStats {
    pub kind: HiddenEventKind,
    pub price: f64,
    pub score: f64,
    pub ts_local_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HiddenEventKind {
    Iceberg,
    Replenishment,
    SpoofPull,
    LiquidationImpulse,
}

pub fn spawn_hidden_engine(
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<HiddenLiquidityStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = HiddenLiquidityTracker::default();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    let stats = match event {
                        MarketEvent::Trade(trade) => tracker.observe_trade(trade),
                        MarketEvent::DepthDelta(delta) => tracker.observe_depth_delta(delta),
                        MarketEvent::Ticker(ticker) => tracker.observe_ticker(ticker),
                        _ => tracker.stats(),
                    };
                    let _ = stats_tx.send(stats);
                }
            }
        }
    })
}

#[derive(Debug, Default)]
pub struct HiddenLiquidityTracker {
    levels: BTreeMap<PriceKey, LevelObservation>,
    trades: VecDeque<SignedTrade>,
    stop_pool_touches: BTreeMap<PriceKey, StopPoolStats>,
    exchange_last_ts: BTreeMap<&'static str, i64>,
    exchanges_seen: BTreeSet<&'static str>,
    recent_events: VecDeque<HiddenEventStats>,
    funding_rate: Option<f64>,
    stats: HiddenLiquidityStats,
}

impl HiddenLiquidityTracker {
    pub fn observe_trade(&mut self, trade: TradeEvent) -> HiddenLiquidityStats {
        self.stats.events_seen += 1;
        self.observe_exchange(trade.exchange, trade.ts_local_ms);
        self.update_stop_pools(trade.price, trade.ts_local_ms);

        let signed = SignedTrade::from_trade(trade);
        self.trades.push_back(signed.clone());
        self.prune_trades(signed.ts_local_ms);
        self.detect_iceberg(&signed);
        self.refresh_impulse_and_crowding();
        self.stats()
    }

    pub fn observe_depth_delta(&mut self, delta: DepthDeltaEvent) -> HiddenLiquidityStats {
        self.stats.events_seen += 1;
        self.observe_exchange(delta.exchange, delta.ts_local_ms);

        for level in delta.bids.iter().chain(delta.asks.iter()) {
            self.observe_level(level.price, level.qty, delta.ts_local_ms);
        }

        self.stats()
    }

    pub fn observe_ticker(&mut self, ticker: TickerEvent) -> HiddenLiquidityStats {
        self.stats.events_seen += 1;
        self.observe_exchange(ticker.exchange, ticker.ts_local_ms);
        self.funding_rate = ticker.funding_rate;
        self.refresh_impulse_and_crowding();
        self.stats()
    }

    pub fn stats(&mut self) -> HiddenLiquidityStats {
        self.stats.exchanges_seen = self.exchanges_seen.len();
        self.stats.cross_exchange_lead_lag = self.lead_lag();
        self.stats.stop_pools = self.top_stop_pools();
        self.stats.recent_events = self.recent_events.iter().cloned().collect();
        self.stats.hidden_liquidity_score =
            self.stats.iceberg_count as f64 + self.recent_replenishment_score();
        self.stats.clone()
    }

    fn observe_exchange(&mut self, exchange: Exchange, ts_local_ms: i64) {
        let name = exchange_name(exchange);
        self.exchanges_seen.insert(name);
        self.exchange_last_ts.insert(name, ts_local_ms);
    }

    fn observe_level(&mut self, price: f64, qty: f64, ts_local_ms: i64) {
        let key = PriceKey(price);
        let previous = self.levels.get(&key).cloned().unwrap_or_default();

        if previous.qty > 0.0 && qty < previous.qty && previous.qty - qty >= LARGE_PULL_QTY {
            let traded_near = self
                .trades
                .iter()
                .any(|trade| trade.ts_local_ms >= ts_local_ms - WINDOW_MS && trade.price == price);
            if !traded_near {
                self.stats.spoof_pull_count += 1;
                self.push_event(HiddenEventStats {
                    kind: HiddenEventKind::SpoofPull,
                    price,
                    score: previous.qty - qty,
                    ts_local_ms,
                });
            }
        }

        if qty > previous.qty && previous.qty > 0.0 {
            self.push_event(HiddenEventStats {
                kind: HiddenEventKind::Replenishment,
                price,
                score: qty - previous.qty,
                ts_local_ms,
            });
        }

        self.levels.insert(key, LevelObservation { qty });
    }

    fn detect_iceberg(&mut self, trade: &SignedTrade) {
        let displayed_qty = self
            .levels
            .get(&PriceKey(trade.price))
            .map(|level| level.qty)
            .unwrap_or_default();
        if displayed_qty <= f64::EPSILON {
            return;
        }

        let traded_qty = self
            .trades
            .iter()
            .filter(|recent| {
                recent.price == trade.price && recent.ts_local_ms >= trade.ts_local_ms - WINDOW_MS
            })
            .map(|recent| recent.qty)
            .sum::<f64>();

        if traded_qty >= displayed_qty * ICEBERG_TRADE_MULTIPLE {
            self.stats.iceberg_count += 1;
            self.push_event(HiddenEventStats {
                kind: HiddenEventKind::Iceberg,
                price: trade.price,
                score: traded_qty / displayed_qty,
                ts_local_ms: trade.ts_local_ms,
            });
        }
    }

    fn update_stop_pools(&mut self, price: f64, ts_local_ms: i64) {
        let rounded = (price / ROUND_NUMBER_SIZE).round() * ROUND_NUMBER_SIZE;
        let candidates = [rounded, price.floor(), price.ceil()];
        for candidate in candidates {
            let key = PriceKey(candidate);
            let pool = self.stop_pool_touches.entry(key).or_insert(StopPoolStats {
                price: candidate,
                touches: 0,
                last_touch_ms: ts_local_ms,
                pool_score: 0.0,
            });
            pool.touches += 1;
            pool.last_touch_ms = ts_local_ms;
            pool.pool_score = pool.touches as f64;
        }

        while self.stop_pool_touches.len() > STOP_POOL_HISTORY {
            if let Some(oldest) = self
                .stop_pool_touches
                .iter()
                .min_by_key(|(_, pool)| pool.last_touch_ms)
                .map(|(price, _)| *price)
            {
                self.stop_pool_touches.remove(&oldest);
            } else {
                break;
            }
        }
    }

    fn refresh_impulse_and_crowding(&mut self) {
        let Some(last_ts) = self.trades.back().map(|trade| trade.ts_local_ms) else {
            return;
        };
        let window = self
            .trades
            .iter()
            .filter(|trade| trade.ts_local_ms >= last_ts - WINDOW_MS)
            .collect::<Vec<_>>();
        if window.is_empty() {
            return;
        }

        let signed_notional = window
            .iter()
            .map(|trade| trade.signed_notional)
            .sum::<f64>();
        let volume = window.iter().map(|trade| trade.notional.abs()).sum::<f64>();
        let price_change = window
            .first()
            .zip(window.last())
            .map(|(first, last)| last.price - first.price)
            .unwrap_or_default();
        let direction_align = if signed_notional.signum() == price_change.signum() {
            1.0
        } else {
            0.5
        };

        self.stats.liquidation_impulse_score =
            signed_notional.abs() / volume.max(1.0) * price_change.abs().max(1.0) * direction_align;
        if self.stats.liquidation_impulse_score >= 1.0 {
            self.push_event(HiddenEventStats {
                kind: HiddenEventKind::LiquidationImpulse,
                price: window.last().map(|trade| trade.price).unwrap_or_default(),
                score: self.stats.liquidation_impulse_score,
                ts_local_ms: last_ts,
            });
        }

        self.stats.crowded_positioning_score = self
            .funding_rate
            .map(|funding| funding * signed_notional.signum())
            .unwrap_or_default();
    }

    fn prune_trades(&mut self, ts_local_ms: i64) {
        while self
            .trades
            .front()
            .is_some_and(|trade| trade.ts_local_ms < ts_local_ms - WINDOW_MS)
        {
            self.trades.pop_front();
        }
    }

    fn recent_replenishment_score(&self) -> f64 {
        self.recent_events
            .iter()
            .filter(|event| matches!(event.kind, HiddenEventKind::Replenishment))
            .map(|event| event.score)
            .sum::<f64>()
    }

    fn top_stop_pools(&self) -> Vec<StopPoolStats> {
        let mut pools = self.stop_pool_touches.values().cloned().collect::<Vec<_>>();
        pools.sort_by(|left, right| {
            right
                .pool_score
                .total_cmp(&left.pool_score)
                .then_with(|| right.last_touch_ms.cmp(&left.last_touch_ms))
        });
        pools.truncate(5);
        pools
    }

    fn lead_lag(&self) -> Option<LeadLagStats> {
        if self.exchange_last_ts.len() < 2 {
            return None;
        }

        let leader = self.exchange_last_ts.iter().max_by_key(|(_, ts)| *ts)?;
        let lagger = self.exchange_last_ts.iter().min_by_key(|(_, ts)| *ts)?;
        Some(LeadLagStats {
            leader: (*leader.0).to_string(),
            lagger: (*lagger.0).to_string(),
            lag_ms: leader.1.saturating_sub(*lagger.1),
        })
    }

    fn push_event(&mut self, event: HiddenEventStats) {
        self.recent_events.push_back(event);
        while self.recent_events.len() > RECENT_EVENTS {
            self.recent_events.pop_front();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct PriceKey(f64);

impl Eq for PriceKey {}

impl PartialOrd for PriceKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PriceKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Debug, Clone, Default)]
struct LevelObservation {
    qty: f64,
}

#[derive(Debug, Clone)]
struct SignedTrade {
    ts_local_ms: i64,
    price: f64,
    qty: f64,
    signed_notional: f64,
    notional: f64,
}

impl SignedTrade {
    fn from_trade(trade: TradeEvent) -> Self {
        let sign = match trade.aggressor_side {
            AggressorSide::Buy => 1.0,
            AggressorSide::Sell => -1.0,
            AggressorSide::Unknown => 0.0,
        };
        let notional = trade.price * trade.qty;
        Self {
            ts_local_ms: trade.ts_local_ms,
            price: trade.price,
            qty: trade.qty,
            signed_notional: sign * notional,
            notional,
        }
    }
}

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Binance => "binance",
        Exchange::Mexc => "mexc",
        Exchange::Deribit => "deribit",
        Exchange::Okx => "okx",
        Exchange::Bybit => "bybit",
        Exchange::Hyperliquid => "hyperliquid",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{DepthLevel, TickerEvent};

    #[test]
    fn detects_iceberg_when_trades_exceed_displayed_qty() {
        let mut tracker = HiddenLiquidityTracker::default();
        tracker.observe_depth_delta(depth(1, 100.0, 1.0));
        tracker.observe_trade(trade(2, 100.0, 1.0, AggressorSide::Buy, Exchange::Binance));
        let stats =
            tracker.observe_trade(trade(3, 100.0, 1.1, AggressorSide::Buy, Exchange::Binance));

        assert_eq!(stats.iceberg_count, 1);
        assert!(stats.hidden_liquidity_score >= 1.0);
    }

    #[test]
    fn detects_spoof_pull_without_recent_trade() {
        let mut tracker = HiddenLiquidityTracker::default();
        tracker.observe_depth_delta(depth(1, 100.0, 10.0));
        let stats = tracker.observe_depth_delta(depth(2, 100.0, 1.0));

        assert_eq!(stats.spoof_pull_count, 1);
        assert!(matches!(
            stats.recent_events[0].kind,
            HiddenEventKind::SpoofPull
        ));
    }

    #[test]
    fn computes_liquidation_impulse_and_crowding() {
        let mut tracker = HiddenLiquidityTracker::default();
        tracker.observe_ticker(TickerEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: 1,
            ts_local_ms: 1,
            bid: 100.0,
            ask: 101.0,
            mark: Some(100.5),
            index: None,
            funding_rate: Some(0.01),
        });
        tracker.observe_trade(trade(2, 100.0, 1.0, AggressorSide::Buy, Exchange::Binance));
        let stats =
            tracker.observe_trade(trade(3, 102.0, 1.0, AggressorSide::Buy, Exchange::Binance));

        assert!(stats.liquidation_impulse_score > 0.0);
        assert!(stats.crowded_positioning_score > 0.0);
    }

    #[test]
    fn tracks_cross_exchange_lead_lag() {
        let mut tracker = HiddenLiquidityTracker::default();
        tracker.observe_trade(trade(10, 100.0, 1.0, AggressorSide::Buy, Exchange::Binance));
        let stats =
            tracker.observe_trade(trade(15, 100.0, 1.0, AggressorSide::Buy, Exchange::Mexc));

        assert_eq!(stats.exchanges_seen, 2);
        assert_eq!(
            stats
                .cross_exchange_lead_lag
                .as_ref()
                .map(|lag| lag.leader.as_str()),
            Some("mexc")
        );
        assert_eq!(
            stats.cross_exchange_lead_lag.as_ref().map(|lag| lag.lag_ms),
            Some(5)
        );
    }

    fn depth(ts_local_ms: i64, price: f64, qty: f64) -> DepthDeltaEvent {
        DepthDeltaEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            first_update_id: Some(1),
            sequence: Some(1),
            previous_sequence: Some(0),
            bids: vec![DepthLevel { price, qty }],
            asks: Vec::new(),
        }
    }

    fn trade(
        ts_local_ms: i64,
        price: f64,
        qty: f64,
        aggressor_side: AggressorSide,
        exchange: Exchange,
    ) -> TradeEvent {
        TradeEvent {
            exchange,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            price,
            qty,
            aggressor_side,
        }
    }
}
