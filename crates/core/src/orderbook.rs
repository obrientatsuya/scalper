use crate::events::{DepthDeltaEvent, DepthLevel, Exchange, MarketEvent};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub exchange: Exchange,
    pub symbol: String,
    pub last_update_id: u64,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookStats {
    pub exchange: Exchange,
    pub symbol: String,
    pub synced: bool,
    pub last_update_id: Option<u64>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub bid_levels: usize,
    pub ask_levels: usize,
    pub buffered_deltas: usize,
    pub applied_deltas: u64,
    pub resyncs: u64,
    pub gaps: u64,
}

impl Default for OrderBookStats {
    fn default() -> Self {
        Self {
            exchange: Exchange::Binance,
            symbol: String::new(),
            synced: false,
            last_update_id: None,
            best_bid: None,
            best_ask: None,
            bid_levels: 0,
            ask_levels: 0,
            buffered_deltas: 0,
            applied_deltas: 0,
            resyncs: 0,
            gaps: 0,
        }
    }
}

pub fn spawn_orderbook_engine(
    symbol: String,
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<OrderBookStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut engine = LocalOrderBook::new(Exchange::Binance, symbol);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    if let MarketEvent::DepthDelta(delta) = event
                        && delta.exchange == Exchange::Binance
                    {
                        match engine.on_depth_delta(delta).await {
                            Ok(()) => {
                                let _ = stats_tx.send(engine.stats());
                            }
                            Err(error) => {
                                warn!(%error, "orderbook delta failed");
                            }
                        }
                    }
                }
            }
        }
    })
}

#[derive(Debug)]
pub struct LocalOrderBook {
    exchange: Exchange,
    symbol: String,
    bids: BTreeMap<PriceKey, f64>,
    asks: BTreeMap<PriceKey, f64>,
    last_update_id: Option<u64>,
    pending: VecDeque<DepthDeltaEvent>,
    synced: bool,
    snapshot_in_flight: bool,
    awaiting_first_delta: bool,
    applied_deltas: u64,
    resyncs: u64,
    gaps: u64,
    http: reqwest::Client,
}

impl LocalOrderBook {
    pub fn new(exchange: Exchange, symbol: String) -> Self {
        Self {
            exchange,
            symbol,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: None,
            pending: VecDeque::new(),
            synced: false,
            snapshot_in_flight: false,
            awaiting_first_delta: false,
            applied_deltas: 0,
            resyncs: 0,
            gaps: 0,
            http: reqwest::Client::new(),
        }
    }

    pub async fn on_depth_delta(&mut self, delta: DepthDeltaEvent) -> anyhow::Result<()> {
        if !self.synced {
            self.pending.push_back(delta);
            if !self.snapshot_in_flight {
                self.sync_from_snapshot().await?;
            }
            return Ok(());
        }

        if !self.apply_synced_delta(delta) {
            self.mark_desynced();
            self.sync_from_snapshot().await?;
        }

        Ok(())
    }

    pub fn apply_snapshot(&mut self, snapshot: OrderBookSnapshot) {
        self.bids.clear();
        self.asks.clear();

        for level in snapshot.bids {
            set_level(&mut self.bids, level);
        }
        for level in snapshot.asks {
            set_level(&mut self.asks, level);
        }

        self.last_update_id = Some(snapshot.last_update_id);
        self.synced = true;
        self.awaiting_first_delta = true;
        self.resyncs += 1;
    }

    pub fn replay_pending(&mut self) {
        let Some(snapshot_id) = self.last_update_id else {
            return;
        };

        while let Some(delta) = self.pending.front() {
            if delta.sequence.unwrap_or_default() < snapshot_id {
                self.pending.pop_front();
            } else {
                break;
            }
        }

        let mut first_applied = false;

        while let Some(delta) = self.pending.pop_front() {
            if !first_applied {
                if !first_delta_matches_snapshot(&delta, snapshot_id) {
                    self.mark_desynced();
                    return;
                }
                self.apply_levels(&delta);
                self.last_update_id = delta.sequence;
                self.awaiting_first_delta = false;
                self.applied_deltas += 1;
                first_applied = true;
                continue;
            }

            if !self.apply_synced_delta(delta) {
                self.mark_desynced();
                return;
            }
        }

        info!(
            last_update_id = ?self.last_update_id,
            bid_levels = self.bids.len(),
            ask_levels = self.asks.len(),
            "orderbook synced"
        );
    }

    pub fn stats(&self) -> OrderBookStats {
        OrderBookStats {
            exchange: self.exchange,
            symbol: self.symbol.clone(),
            synced: self.synced && !self.awaiting_first_delta,
            last_update_id: self.last_update_id,
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            bid_levels: self.bids.len(),
            ask_levels: self.asks.len(),
            buffered_deltas: self.pending.len(),
            applied_deltas: self.applied_deltas,
            resyncs: self.resyncs,
            gaps: self.gaps,
        }
    }

    fn apply_synced_delta(&mut self, delta: DepthDeltaEvent) -> bool {
        let Some(last_update_id) = self.last_update_id else {
            return false;
        };

        if self.awaiting_first_delta {
            if is_before_snapshot(&delta, last_update_id) {
                return true;
            }

            if !first_delta_matches_snapshot(&delta, last_update_id) {
                warn!(
                    snapshot_id = last_update_id,
                    first_update_id = ?delta.first_update_id,
                    final_update_id = ?delta.sequence,
                    previous_sequence = ?delta.previous_sequence,
                    "first orderbook delta did not match snapshot"
                );
                return false;
            }
            self.apply_levels(&delta);
            self.last_update_id = delta.sequence;
            self.awaiting_first_delta = false;
            self.applied_deltas += 1;
            return true;
        }

        let previous_ok = match delta.previous_sequence {
            Some(previous_sequence) => previous_sequence == last_update_id,
            None => delta.first_update_id.unwrap_or_default() <= last_update_id + 1,
        };

        if !previous_ok || delta.sequence.unwrap_or_default() <= last_update_id {
            warn!(
                last_update_id,
                first_update_id = ?delta.first_update_id,
                final_update_id = ?delta.sequence,
                previous_sequence = ?delta.previous_sequence,
                previous_ok,
                "orderbook sequence gap"
            );
            return false;
        }

        self.apply_levels(&delta);
        self.last_update_id = delta.sequence;
        self.applied_deltas += 1;
        true
    }

    fn apply_levels(&mut self, delta: &DepthDeltaEvent) {
        for level in &delta.bids {
            set_level(&mut self.bids, level.clone());
        }

        for level in &delta.asks {
            set_level(&mut self.asks, level.clone());
        }
    }

    async fn sync_from_snapshot(&mut self) -> anyhow::Result<()> {
        self.snapshot_in_flight = true;
        let snapshot = fetch_binance_snapshot(&self.http, &self.symbol).await?;
        self.snapshot_in_flight = false;
        self.apply_snapshot(snapshot);
        self.replay_pending();
        Ok(())
    }

    fn mark_desynced(&mut self) {
        self.synced = false;
        self.awaiting_first_delta = false;
        self.gaps += 1;
        self.last_update_id = None;
        self.bids.clear();
        self.asks.clear();
        self.pending.clear();
        warn!("orderbook desynced; resync required");
    }

    fn best_bid(&self) -> Option<f64> {
        self.bids.keys().next_back().map(|price| price.0)
    }

    fn best_ask(&self) -> Option<f64> {
        self.asks.keys().next().map(|price| price.0)
    }
}

fn covers_snapshot(delta: &DepthDeltaEvent, snapshot_id: u64) -> bool {
    let first = delta.first_update_id.unwrap_or_default();
    let final_id = delta.sequence.unwrap_or_default();
    first <= snapshot_id && final_id >= snapshot_id
}

fn is_before_snapshot(delta: &DepthDeltaEvent, snapshot_id: u64) -> bool {
    delta.sequence.unwrap_or_default() < snapshot_id
}

fn first_delta_matches_snapshot(delta: &DepthDeltaEvent, snapshot_id: u64) -> bool {
    covers_snapshot(delta, snapshot_id)
        || delta.previous_sequence == Some(snapshot_id)
        || delta.first_update_id == Some(snapshot_id + 1)
}

fn set_level(side: &mut BTreeMap<PriceKey, f64>, level: DepthLevel) {
    let key = PriceKey(level.price);
    if level.qty == 0.0 {
        side.remove(&key);
    } else {
        side.insert(key, level.qty);
    }
}

async fn fetch_binance_snapshot(
    http: &reqwest::Client,
    symbol: &str,
) -> anyhow::Result<OrderBookSnapshot> {
    let url = format!("https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit=1000");
    let response: BinanceDepthSnapshot = http
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    Ok(OrderBookSnapshot {
        exchange: Exchange::Binance,
        symbol: symbol.to_string(),
        last_update_id: response.last_update_id,
        bids: parse_snapshot_levels(response.bids)?,
        asks: parse_snapshot_levels(response.asks)?,
    })
}

fn parse_snapshot_levels(levels: Vec<[String; 2]>) -> anyhow::Result<Vec<DepthLevel>> {
    levels
        .into_iter()
        .map(|[price, qty]| {
            Ok(DepthLevel {
                price: price.parse()?,
                qty: qty.parse()?,
            })
        })
        .collect()
}

#[derive(Debug, Deserialize)]
struct BinanceDepthSnapshot {
    #[serde(rename = "lastUpdateId")]
    last_update_id: u64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn delta(first: u64, final_id: u64, prev: u64, bid_qty: f64) -> DepthDeltaEvent {
        DepthDeltaEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: 1,
            ts_local_ms: 1,
            first_update_id: Some(first),
            sequence: Some(final_id),
            previous_sequence: Some(prev),
            bids: vec![DepthLevel {
                price: 100.0,
                qty: bid_qty,
            }],
            asks: vec![DepthLevel {
                price: 101.0,
                qty: 1.0,
            }],
        }
    }

    #[test]
    fn snapshot_cover_rule_matches_binance_bootstrap() {
        assert!(covers_snapshot(&delta(100, 105, 99, 1.0), 100));
        assert!(covers_snapshot(&delta(99, 105, 98, 1.0), 100));
        assert!(!covers_snapshot(&delta(101, 105, 99, 1.0), 100));
        assert!(covers_snapshot(&delta(90, 100, 99, 1.0), 100));
    }

    #[test]
    fn first_delta_allows_next_update_after_snapshot() {
        assert!(first_delta_matches_snapshot(
            &delta(101, 105, 100, 1.0),
            100
        ));
        assert!(first_delta_matches_snapshot(&delta(100, 105, 99, 1.0), 100));
        assert!(!first_delta_matches_snapshot(
            &delta(102, 105, 101, 1.0),
            100
        ));
    }

    #[test]
    fn applies_synced_delta_when_previous_matches() {
        let mut book = LocalOrderBook::new(Exchange::Binance, "BTCUSDT".to_string());
        book.apply_snapshot(OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            last_update_id: 100,
            bids: vec![DepthLevel {
                price: 99.0,
                qty: 1.0,
            }],
            asks: vec![DepthLevel {
                price: 101.0,
                qty: 1.0,
            }],
        });
        book.awaiting_first_delta = false;

        assert!(book.apply_synced_delta(delta(101, 101, 100, 2.0)));
        assert_eq!(book.best_bid(), Some(100.0));
        assert_eq!(book.last_update_id, Some(101));
    }

    #[test]
    fn rejects_gap() {
        let mut book = LocalOrderBook::new(Exchange::Binance, "BTCUSDT".to_string());
        book.apply_snapshot(OrderBookSnapshot {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            last_update_id: 100,
            bids: vec![],
            asks: vec![],
        });

        assert!(!book.apply_synced_delta(delta(102, 102, 101, 2.0)));
    }
}
