use crate::events::{AggressorSide, MarketEvent, TradeEvent};
use serde::Serialize;
use std::collections::{BTreeMap, VecDeque};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const WINDOWS_MS: [i64; 5] = [250, 1_000, 5_000, 15_000, 60_000];
const MAX_WINDOW_MS: i64 = 60_000;
const MAX_DELTA_SAMPLES: usize = 512;
const FOOTPRINT_BUCKET_MS: i64 = 1_000;
const FOOTPRINT_PRICE_BIN_SIZE: f64 = 1.0;

#[derive(Debug, Clone, Default, Serialize)]
pub struct OrderflowStats {
    pub trades_seen: u64,
    pub cvd_notional: f64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub windows: Vec<OrderflowWindowStats>,
    pub footprint: Option<FootprintStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OrderflowWindowStats {
    pub window_ms: i64,
    pub trades: u64,
    pub buy_notional: f64,
    pub sell_notional: f64,
    pub delta_notional: f64,
    pub volume_notional: f64,
    pub first_price: Option<f64>,
    pub last_price: Option<f64>,
    pub price_change: Option<f64>,
    pub price_efficiency: Option<f64>,
    pub delta_z: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FootprintStats {
    pub bucket_start_ms: i64,
    pub bucket_ms: i64,
    pub price_bin_size: f64,
    pub total_bid_qty: f64,
    pub total_ask_qty: f64,
    pub delta_qty: f64,
    pub poc_price_bin: Option<f64>,
    pub levels: Vec<FootprintLevelStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FootprintLevelStats {
    pub price_bin: f64,
    pub bid_qty: f64,
    pub ask_qty: f64,
    pub delta_qty: f64,
}

#[derive(Debug, Clone)]
pub struct OrderflowConfig {
    pub footprint_bucket_ms: i64,
    pub footprint_price_bin_size: f64,
}

impl Default for OrderflowConfig {
    fn default() -> Self {
        Self {
            footprint_bucket_ms: FOOTPRINT_BUCKET_MS,
            footprint_price_bin_size: FOOTPRINT_PRICE_BIN_SIZE,
        }
    }
}

pub fn spawn_orderflow_engine(
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<OrderflowStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = OrderflowTracker::default();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    if let MarketEvent::Trade(trade) = event {
                        let stats = tracker.observe_trade(trade);
                        let _ = stats_tx.send(stats);
                    }
                }
            }
        }
    })
}

#[derive(Debug, Clone)]
pub struct OrderflowTracker {
    config: OrderflowConfig,
    trades: VecDeque<SignedTrade>,
    delta_samples: Vec<WindowDeltaSamples>,
    footprint_bucket_start_ms: Option<i64>,
    footprint_levels: BTreeMap<i64, FootprintLevel>,
    stats: OrderflowStats,
}

impl Default for OrderflowTracker {
    fn default() -> Self {
        Self::new(OrderflowConfig::default())
    }
}

impl OrderflowTracker {
    pub fn new(config: OrderflowConfig) -> Self {
        Self {
            config,
            trades: VecDeque::new(),
            delta_samples: WINDOWS_MS
                .iter()
                .map(|window_ms| WindowDeltaSamples::new(*window_ms))
                .collect(),
            footprint_bucket_start_ms: None,
            footprint_levels: BTreeMap::new(),
            stats: OrderflowStats::default(),
        }
    }

    pub fn observe_trade(&mut self, trade: TradeEvent) -> OrderflowStats {
        let signed = SignedTrade::from_trade(trade);

        self.stats.trades_seen += 1;
        self.stats.cvd_notional += signed.signed_notional;
        if signed.signed_notional > 0.0 {
            self.stats.buy_notional += signed.abs_notional;
        } else if signed.signed_notional < 0.0 {
            self.stats.sell_notional += signed.abs_notional;
        }

        self.update_footprint(&signed);
        self.trades.push_back(signed);
        self.prune_old_trades();

        self.stats.windows = WINDOWS_MS
            .iter()
            .map(|window_ms| self.window_stats(*window_ms))
            .collect();
        self.stats.footprint = self.footprint_stats();

        for sample in &mut self.delta_samples {
            if let Some(window) = self
                .stats
                .windows
                .iter()
                .find(|window| window.window_ms == sample.window_ms)
            {
                sample.push(window.delta_notional);
            }
        }

        self.stats.clone()
    }

    fn prune_old_trades(&mut self) {
        let Some(latest_ts_ms) = self.trades.back().map(|trade| trade.ts_local_ms) else {
            return;
        };
        let cutoff_ms = latest_ts_ms - MAX_WINDOW_MS;
        while self
            .trades
            .front()
            .is_some_and(|trade| trade.ts_local_ms < cutoff_ms)
        {
            self.trades.pop_front();
        }
    }

    fn window_stats(&self, window_ms: i64) -> OrderflowWindowStats {
        let Some(latest_ts_ms) = self.trades.back().map(|trade| trade.ts_local_ms) else {
            return empty_window(window_ms);
        };
        let cutoff_ms = latest_ts_ms - window_ms;

        let mut trades = 0;
        let mut buy_notional = 0.0;
        let mut sell_notional = 0.0;
        let mut delta_notional = 0.0;
        let mut volume_notional = 0.0;
        let mut first_price = None;
        let mut last_price = None;

        for trade in self
            .trades
            .iter()
            .filter(|trade| trade.ts_local_ms >= cutoff_ms)
        {
            trades += 1;
            delta_notional += trade.signed_notional;
            volume_notional += trade.abs_notional;
            if trade.signed_notional > 0.0 {
                buy_notional += trade.abs_notional;
            } else if trade.signed_notional < 0.0 {
                sell_notional += trade.abs_notional;
            }
            first_price.get_or_insert(trade.price);
            last_price = Some(trade.price);
        }

        let price_change = first_price
            .zip(last_price)
            .map(|(first, last)| last - first);
        let price_efficiency =
            price_change.map(|change: f64| change.abs() / delta_notional.abs().max(1.0));
        let delta_z = self
            .delta_samples
            .iter()
            .find(|sample| sample.window_ms == window_ms)
            .and_then(|sample| sample.z_score(delta_notional));

        OrderflowWindowStats {
            window_ms,
            trades,
            buy_notional,
            sell_notional,
            delta_notional,
            volume_notional,
            first_price,
            last_price,
            price_change,
            price_efficiency,
            delta_z,
        }
    }

    fn update_footprint(&mut self, trade: &SignedTrade) {
        let bucket_start_ms = trade.ts_local_ms
            - trade
                .ts_local_ms
                .rem_euclid(self.config.footprint_bucket_ms);
        if self.footprint_bucket_start_ms != Some(bucket_start_ms) {
            self.footprint_bucket_start_ms = Some(bucket_start_ms);
            self.footprint_levels.clear();
        }

        let price_bin = (trade.price / self.config.footprint_price_bin_size).floor() as i64;
        let level = self.footprint_levels.entry(price_bin).or_default();
        if trade.signed_notional > 0.0 {
            level.ask_qty += trade.qty;
        } else if trade.signed_notional < 0.0 {
            level.bid_qty += trade.qty;
        }
    }

    fn footprint_stats(&self) -> Option<FootprintStats> {
        let bucket_start_ms = self.footprint_bucket_start_ms?;
        let mut total_bid_qty = 0.0;
        let mut total_ask_qty = 0.0;
        let mut poc_price_bin = None;
        let mut poc_volume = 0.0;

        let levels = self
            .footprint_levels
            .iter()
            .map(|(price_bin, level)| {
                let price_bin = *price_bin as f64 * self.config.footprint_price_bin_size;
                let total = level.bid_qty + level.ask_qty;
                if total > poc_volume {
                    poc_volume = total;
                    poc_price_bin = Some(price_bin);
                }
                total_bid_qty += level.bid_qty;
                total_ask_qty += level.ask_qty;

                FootprintLevelStats {
                    price_bin,
                    bid_qty: level.bid_qty,
                    ask_qty: level.ask_qty,
                    delta_qty: level.ask_qty - level.bid_qty,
                }
            })
            .collect();

        Some(FootprintStats {
            bucket_start_ms,
            bucket_ms: self.config.footprint_bucket_ms,
            price_bin_size: self.config.footprint_price_bin_size,
            total_bid_qty,
            total_ask_qty,
            delta_qty: total_ask_qty - total_bid_qty,
            poc_price_bin,
            levels,
        })
    }
}

#[derive(Debug, Clone)]
struct SignedTrade {
    ts_local_ms: i64,
    price: f64,
    qty: f64,
    signed_notional: f64,
    abs_notional: f64,
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
            abs_notional: notional,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct FootprintLevel {
    bid_qty: f64,
    ask_qty: f64,
}

#[derive(Debug, Clone)]
struct WindowDeltaSamples {
    window_ms: i64,
    values: VecDeque<f64>,
}

impl WindowDeltaSamples {
    fn new(window_ms: i64) -> Self {
        Self {
            window_ms,
            values: VecDeque::new(),
        }
    }

    fn push(&mut self, value: f64) {
        self.values.push_back(value);
        while self.values.len() > MAX_DELTA_SAMPLES {
            self.values.pop_front();
        }
    }

    fn z_score(&self, value: f64) -> Option<f64> {
        if self.values.len() < 2 {
            return None;
        }

        let mean = self.values.iter().sum::<f64>() / self.values.len() as f64;
        let variance = self
            .values
            .iter()
            .map(|sample| (sample - mean).powi(2))
            .sum::<f64>()
            / self.values.len() as f64;
        let std = variance.sqrt();
        if std <= f64::EPSILON {
            return None;
        }

        Some((value - mean) / std)
    }
}

fn empty_window(window_ms: i64) -> OrderflowWindowStats {
    OrderflowWindowStats {
        window_ms,
        trades: 0,
        buy_notional: 0.0,
        sell_notional: 0.0,
        delta_notional: 0.0,
        volume_notional: 0.0,
        first_price: None,
        last_price: None,
        price_change: None,
        price_efficiency: None,
        delta_z: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Exchange;

    #[test]
    fn tracks_cvd_from_signed_trades() {
        let mut tracker = OrderflowTracker::default();
        tracker.observe_trade(trade(1, 100.0, 2.0, AggressorSide::Buy));
        let stats = tracker.observe_trade(trade(2, 100.0, 0.5, AggressorSide::Sell));

        assert_eq!(stats.trades_seen, 2);
        assert_eq!(stats.buy_notional, 200.0);
        assert_eq!(stats.sell_notional, 50.0);
        assert_eq!(stats.cvd_notional, 150.0);
    }

    #[test]
    fn builds_multi_window_delta_and_efficiency() {
        let mut tracker = OrderflowTracker::default();
        tracker.observe_trade(trade(0, 100.0, 1.0, AggressorSide::Buy));
        tracker.observe_trade(trade(900, 101.0, 2.0, AggressorSide::Sell));
        let stats = tracker.observe_trade(trade(1_200, 102.0, 1.0, AggressorSide::Buy));

        let one_second = stats
            .windows
            .iter()
            .find(|window| window.window_ms == 1_000)
            .expect("1s window");

        assert_eq!(one_second.trades, 2);
        assert_eq!(one_second.delta_notional, -100.0);
        assert_eq!(one_second.volume_notional, 304.0);
        assert_eq!(one_second.price_change, Some(1.0));
        assert_eq!(one_second.price_efficiency, Some(0.01));
    }

    #[test]
    fn builds_footprint_bucket() {
        let mut tracker = OrderflowTracker::new(OrderflowConfig {
            footprint_bucket_ms: 1_000,
            footprint_price_bin_size: 1.0,
        });
        tracker.observe_trade(trade(1_001, 100.2, 1.0, AggressorSide::Buy));
        let stats = tracker.observe_trade(trade(1_500, 100.8, 0.25, AggressorSide::Sell));
        let footprint = stats.footprint.expect("footprint");

        assert_eq!(footprint.bucket_start_ms, 1_000);
        assert_eq!(footprint.total_ask_qty, 1.0);
        assert_eq!(footprint.total_bid_qty, 0.25);
        assert_eq!(footprint.delta_qty, 0.75);
        assert_eq!(footprint.levels.len(), 1);
        assert_eq!(footprint.levels[0].price_bin, 100.0);
    }

    fn trade(ts_local_ms: i64, price: f64, qty: f64, side: AggressorSide) -> TradeEvent {
        TradeEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            price,
            qty,
            aggressor_side: side,
        }
    }
}
