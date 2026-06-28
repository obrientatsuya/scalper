use crate::events::{DepthDeltaEvent, DepthLevel, Exchange, MarketEvent, OrderBookSnapshotEvent};
use serde::Serialize;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const TOP_N_LEVELS: usize = 20;
const EVENT_HISTORY: usize = 32;
const NEAR_TOP_LEVELS: usize = 20;

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

#[derive(Debug, Clone, Default, Serialize)]
pub struct HeatmapStats {
    pub events_seen: u64,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub spread: Option<f64>,
    pub microprice: Option<f64>,
    pub obi_5: Option<f64>,
    pub obi_10: Option<f64>,
    pub obi_20: Option<f64>,
    pub bid_wall: Option<WallStats>,
    pub ask_wall: Option<WallStats>,
    pub stack_bid_count: u64,
    pub pull_bid_count: u64,
    pub stack_ask_count: u64,
    pub pull_ask_count: u64,
    pub recent_events: Vec<HeatmapEventStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WallStats {
    pub side: BookSide,
    pub price: f64,
    pub qty: f64,
    pub size_z: f64,
    pub age_ms: i64,
    pub wall_quality: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct HeatmapEventStats {
    pub kind: HeatmapEventKind,
    pub side: BookSide,
    pub price: f64,
    pub qty_before: f64,
    pub qty_after: f64,
    pub delta_qty: f64,
    pub ts_local_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HeatmapEventKind {
    Stack,
    Pull,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BookSide {
    Bid,
    Ask,
}

pub fn spawn_heatmap_engine(
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<HeatmapStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = HeatmapTracker::default();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    match event {
                        MarketEvent::OrderBookSnapshot(snapshot) if snapshot.exchange == Exchange::Binance => {
                            let stats = tracker.apply_snapshot(snapshot);
                            let _ = stats_tx.send(stats);
                        }
                        MarketEvent::DepthDelta(delta) if delta.exchange == Exchange::Binance => {
                            let stats = tracker.apply_delta(delta);
                            let _ = stats_tx.send(stats);
                        }
                        _ => {}
                    }
                }
            }
        }
    })
}

#[derive(Debug, Default)]
pub struct HeatmapTracker {
    bids: BTreeMap<PriceKey, f64>,
    asks: BTreeMap<PriceKey, f64>,
    bid_states: BTreeMap<PriceKey, LevelState>,
    ask_states: BTreeMap<PriceKey, LevelState>,
    recent_events: VecDeque<HeatmapEventStats>,
    stats: HeatmapStats,
}

impl HeatmapTracker {
    pub fn apply_snapshot(&mut self, snapshot: OrderBookSnapshotEvent) -> HeatmapStats {
        self.bids.clear();
        self.asks.clear();
        self.bid_states.clear();
        self.ask_states.clear();
        self.recent_events.clear();
        self.stats = HeatmapStats::default();
        self.stats.events_seen = 1;

        for level in snapshot.bids {
            apply_snapshot_level(
                &mut self.bids,
                &mut self.bid_states,
                level,
                snapshot.ts_local_ms,
            );
        }
        for level in snapshot.asks {
            apply_snapshot_level(
                &mut self.asks,
                &mut self.ask_states,
                level,
                snapshot.ts_local_ms,
            );
        }

        self.refresh_stats(snapshot.ts_local_ms)
    }

    pub fn apply_delta(&mut self, delta: DepthDeltaEvent) -> HeatmapStats {
        self.stats.events_seen += 1;
        let ts_local_ms = delta.ts_local_ms;

        for level in delta.bids {
            self.apply_level(BookSide::Bid, level, ts_local_ms);
        }
        for level in delta.asks {
            self.apply_level(BookSide::Ask, level, ts_local_ms);
        }

        self.refresh_stats(ts_local_ms)
    }

    fn apply_level(&mut self, side: BookSide, level: DepthLevel, ts_local_ms: i64) {
        let key = PriceKey(level.price);
        let before_qty = self.qty(side, key);
        let after_qty = level.qty;
        let touched = self.is_at_touch(side, key);
        let was_near_top = self.is_near_top(side, key, NEAR_TOP_LEVELS);

        match side {
            BookSide::Bid => {
                set_level(&mut self.bids, key, after_qty);
                update_state(
                    &mut self.bid_states,
                    key,
                    before_qty,
                    after_qty,
                    ts_local_ms,
                    touched,
                );
            }
            BookSide::Ask => {
                set_level(&mut self.asks, key, after_qty);
                update_state(
                    &mut self.ask_states,
                    key,
                    before_qty,
                    after_qty,
                    ts_local_ms,
                    touched,
                );
            }
        }

        let is_near_top = self.is_near_top(side, key, NEAR_TOP_LEVELS);
        self.record_stack_pull(
            side,
            key,
            before_qty,
            after_qty,
            was_near_top || is_near_top,
            ts_local_ms,
        );
    }

    fn record_stack_pull(
        &mut self,
        side: BookSide,
        key: PriceKey,
        before_qty: f64,
        after_qty: f64,
        near_top: bool,
        ts_local_ms: i64,
    ) {
        if !near_top {
            return;
        }

        let kind = if after_qty > before_qty && before_qty > 0.0 {
            Some(HeatmapEventKind::Stack)
        } else if before_qty > 0.0 && after_qty < before_qty * 0.5 {
            Some(HeatmapEventKind::Pull)
        } else {
            None
        };

        let Some(kind) = kind else {
            return;
        };

        match (kind, side) {
            (HeatmapEventKind::Stack, BookSide::Bid) => self.stats.stack_bid_count += 1,
            (HeatmapEventKind::Stack, BookSide::Ask) => self.stats.stack_ask_count += 1,
            (HeatmapEventKind::Pull, BookSide::Bid) => self.stats.pull_bid_count += 1,
            (HeatmapEventKind::Pull, BookSide::Ask) => self.stats.pull_ask_count += 1,
        }

        self.recent_events.push_back(HeatmapEventStats {
            kind,
            side,
            price: key.0,
            qty_before: before_qty,
            qty_after: after_qty,
            delta_qty: after_qty - before_qty,
            ts_local_ms,
        });
        while self.recent_events.len() > EVENT_HISTORY {
            self.recent_events.pop_front();
        }
    }

    fn refresh_stats(&mut self, ts_local_ms: i64) -> HeatmapStats {
        self.stats.best_bid = self.best_bid();
        self.stats.best_ask = self.best_ask();
        self.stats.spread = self
            .stats
            .best_bid
            .zip(self.stats.best_ask)
            .map(|(bid, ask)| ask - bid);
        self.stats.microprice = self.microprice();
        self.stats.obi_5 = self.obi(5);
        self.stats.obi_10 = self.obi(10);
        self.stats.obi_20 = self.obi(20);
        self.stats.bid_wall = self.wall(BookSide::Bid, ts_local_ms);
        self.stats.ask_wall = self.wall(BookSide::Ask, ts_local_ms);
        self.stats.recent_events = self.recent_events.iter().cloned().collect();
        self.stats.clone()
    }

    fn qty(&self, side: BookSide, key: PriceKey) -> f64 {
        match side {
            BookSide::Bid => self.bids.get(&key),
            BookSide::Ask => self.asks.get(&key),
        }
        .copied()
        .unwrap_or_default()
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|price| price.0)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|price| price.0)
    }

    fn is_at_touch(&self, side: BookSide, key: PriceKey) -> bool {
        match side {
            BookSide::Bid => self.bids.keys().next_back() == Some(&key),
            BookSide::Ask => self.asks.keys().next() == Some(&key),
        }
    }

    fn is_near_top(&self, side: BookSide, key: PriceKey, levels: usize) -> bool {
        match side {
            BookSide::Bid => self
                .bids
                .keys()
                .rev()
                .take(levels)
                .any(|price| *price == key),
            BookSide::Ask => self.asks.keys().take(levels).any(|price| *price == key),
        }
    }

    fn microprice(&self) -> Option<f64> {
        let best_bid = self.bids.iter().next_back()?;
        let best_ask = self.asks.iter().next()?;
        let bid_price = best_bid.0.0;
        let bid_qty = *best_bid.1;
        let ask_price = best_ask.0.0;
        let ask_qty = *best_ask.1;
        let denominator = bid_qty + ask_qty;
        if denominator <= f64::EPSILON {
            return None;
        }
        Some((ask_price * bid_qty + bid_price * ask_qty) / denominator)
    }

    fn obi(&self, levels: usize) -> Option<f64> {
        let bid_qty = self.bids.values().rev().take(levels).copied().sum::<f64>();
        let ask_qty = self.asks.values().take(levels).copied().sum::<f64>();
        let denominator = bid_qty + ask_qty;
        if denominator <= f64::EPSILON {
            return None;
        }
        Some((bid_qty - ask_qty) / denominator)
    }

    fn wall(&self, side: BookSide, ts_local_ms: i64) -> Option<WallStats> {
        let levels = match side {
            BookSide::Bid => self
                .bids
                .iter()
                .rev()
                .take(TOP_N_LEVELS)
                .map(|(price, qty)| (*price, *qty))
                .collect::<Vec<_>>(),
            BookSide::Ask => self
                .asks
                .iter()
                .take(TOP_N_LEVELS)
                .map(|(price, qty)| (*price, *qty))
                .collect::<Vec<_>>(),
        };
        if levels.is_empty() {
            return None;
        }

        let mean = levels.iter().map(|(_, qty)| qty).sum::<f64>() / levels.len() as f64;
        let variance = levels
            .iter()
            .map(|(_, qty)| (qty - mean).powi(2))
            .sum::<f64>()
            / levels.len() as f64;
        let std = variance.sqrt();

        levels
            .into_iter()
            .map(|(price, qty)| {
                let size_z = if std <= f64::EPSILON {
                    0.0
                } else {
                    (qty - mean) / std
                };
                let state = match side {
                    BookSide::Bid => self.bid_states.get(&price),
                    BookSide::Ask => self.ask_states.get(&price),
                };
                let age_ms = state
                    .map(|state| ts_local_ms.saturating_sub(state.first_seen_ts))
                    .unwrap_or_default();
                let touch_survival = state
                    .map(LevelState::touch_survival_rate)
                    .unwrap_or_default();
                let vanish_rate = state
                    .map(LevelState::vanish_before_touch_rate)
                    .unwrap_or_default();
                let persistence_score = (age_ms as f64 / 2_000.0).min(2.0);
                let wall_quality = size_z + persistence_score + touch_survival - vanish_rate;

                WallStats {
                    side,
                    price: price.0,
                    qty,
                    size_z,
                    age_ms,
                    wall_quality,
                }
            })
            .max_by(|left, right| left.wall_quality.total_cmp(&right.wall_quality))
    }
}

#[derive(Debug, Clone, Default)]
struct LevelState {
    qty: f64,
    first_seen_ts: i64,
    last_update_ts: i64,
    cumulative_added_qty: f64,
    cumulative_removed_qty: f64,
    touches: u64,
    survived_touches: u64,
    vanished_before_touch: u64,
}

impl LevelState {
    fn touch_survival_rate(&self) -> f64 {
        if self.touches == 0 {
            return 0.0;
        }
        self.survived_touches as f64 / self.touches as f64
    }

    fn vanish_before_touch_rate(&self) -> f64 {
        let total = self.vanished_before_touch + self.survived_touches;
        if total == 0 {
            return 0.0;
        }
        self.vanished_before_touch as f64 / total as f64
    }
}

fn apply_snapshot_level(
    book: &mut BTreeMap<PriceKey, f64>,
    states: &mut BTreeMap<PriceKey, LevelState>,
    level: DepthLevel,
    ts_local_ms: i64,
) {
    if level.qty <= 0.0 {
        return;
    }
    let key = PriceKey(level.price);
    book.insert(key, level.qty);
    states.insert(
        key,
        LevelState {
            qty: level.qty,
            first_seen_ts: ts_local_ms,
            last_update_ts: ts_local_ms,
            cumulative_added_qty: level.qty,
            ..Default::default()
        },
    );
}

fn set_level(book: &mut BTreeMap<PriceKey, f64>, key: PriceKey, qty: f64) {
    if qty <= 0.0 {
        book.remove(&key);
    } else {
        book.insert(key, qty);
    }
}

fn update_state(
    states: &mut BTreeMap<PriceKey, LevelState>,
    key: PriceKey,
    before_qty: f64,
    after_qty: f64,
    ts_local_ms: i64,
    touched: bool,
) {
    if after_qty <= 0.0 && before_qty <= 0.0 {
        return;
    }

    let state = states.entry(key).or_insert_with(|| LevelState {
        first_seen_ts: ts_local_ms,
        last_update_ts: ts_local_ms,
        ..Default::default()
    });

    if after_qty > before_qty {
        state.cumulative_added_qty += after_qty - before_qty;
    } else if before_qty > after_qty {
        state.cumulative_removed_qty += before_qty - after_qty;
    }

    if touched {
        state.touches += 1;
        if after_qty > 0.0 {
            state.survived_touches += 1;
        }
    } else if before_qty > 0.0 && after_qty <= 0.0 && state.touches == 0 {
        state.vanished_before_touch += 1;
    }

    state.qty = after_qty;
    state.last_update_ts = ts_local_ms;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_obi_and_microprice_from_snapshot() {
        let mut tracker = HeatmapTracker::default();
        let stats = tracker.apply_snapshot(snapshot(vec![(100.0, 2.0)], vec![(101.0, 1.0)]));

        assert_eq!(stats.best_bid, Some(100.0));
        assert_eq!(stats.best_ask, Some(101.0));
        assert_eq!(stats.spread, Some(1.0));
        assert_eq!(stats.microprice, Some(100.66666666666667));
        assert_eq!(stats.obi_5, Some(1.0 / 3.0));
    }

    #[test]
    fn detects_near_top_stack_and_pull() {
        let mut tracker = HeatmapTracker::default();
        tracker.apply_snapshot(snapshot(vec![(100.0, 1.0)], vec![(101.0, 1.0)]));

        let stack = tracker.apply_delta(delta(
            2,
            vec![DepthLevel {
                price: 100.0,
                qty: 3.0,
            }],
            vec![],
        ));
        assert_eq!(stack.stack_bid_count, 1);
        assert!(matches!(
            stack.recent_events[0].kind,
            HeatmapEventKind::Stack
        ));

        let pull = tracker.apply_delta(delta(
            3,
            vec![DepthLevel {
                price: 100.0,
                qty: 0.0,
            }],
            vec![],
        ));
        assert_eq!(pull.pull_bid_count, 1);
    }

    #[test]
    fn identifies_wall_candidate() {
        let mut tracker = HeatmapTracker::default();
        let stats = tracker.apply_snapshot(snapshot(
            vec![(100.0, 1.0), (99.0, 10.0), (98.0, 1.0)],
            vec![(101.0, 1.0), (102.0, 1.0)],
        ));

        let wall = stats.bid_wall.expect("bid wall");
        assert_eq!(wall.price, 99.0);
        assert!(wall.size_z > 1.0);
    }

    fn snapshot(bids: Vec<(f64, f64)>, asks: Vec<(f64, f64)>) -> OrderBookSnapshotEvent {
        OrderBookSnapshotEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_local_ms: 1,
            last_update_id: 100,
            bids: bids
                .into_iter()
                .map(|(price, qty)| DepthLevel { price, qty })
                .collect(),
            asks: asks
                .into_iter()
                .map(|(price, qty)| DepthLevel { price, qty })
                .collect(),
        }
    }

    fn delta(ts_local_ms: i64, bids: Vec<DepthLevel>, asks: Vec<DepthLevel>) -> DepthDeltaEvent {
        DepthDeltaEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            first_update_id: Some(101),
            sequence: Some(101),
            previous_sequence: Some(100),
            bids,
            asks,
        }
    }
}
