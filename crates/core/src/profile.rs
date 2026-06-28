use crate::events::{MarketEvent, TradeEvent};
use serde::Serialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const DEFAULT_PRICE_BIN_SIZE: f64 = 1.0;
const DEFAULT_TPO_INTERVAL_MS: i64 = 30_000;
const SESSION_MS: i64 = 86_400_000;
const VALUE_AREA_PCT: f64 = 0.70;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ProfileStats {
    pub session_start_ms: i64,
    pub trades_seen: u64,
    pub price_bin_size: f64,
    pub tpo_interval_ms: i64,
    pub tpo: TpoStats,
    pub volume_profile: VolumeProfileStats,
    pub vwap: VwapStats,
    pub previous_npoc: Option<NakedPocStats>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct TpoStats {
    pub total_tpos: u64,
    pub poc: Option<f64>,
    pub value_area_high: Option<f64>,
    pub value_area_low: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VolumeProfileStats {
    pub total_qty: f64,
    pub vpoc: Option<f64>,
    pub hvn: Vec<VolumeNodeStats>,
    pub lvn: Vec<VolumeNodeStats>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeNodeStats {
    pub price_bin: f64,
    pub qty: f64,
    pub z_score: f64,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct VwapStats {
    pub value: Option<f64>,
    pub sigma: Option<f64>,
    pub upper_1: Option<f64>,
    pub lower_1: Option<f64>,
    pub upper_2: Option<f64>,
    pub lower_2: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct NakedPocStats {
    pub session_start_ms: i64,
    pub price_bin: f64,
    pub touched: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileConfig {
    pub price_bin_size: f64,
    pub tpo_interval: Duration,
}

impl Default for ProfileConfig {
    fn default() -> Self {
        Self {
            price_bin_size: DEFAULT_PRICE_BIN_SIZE,
            tpo_interval: Duration::from_millis(DEFAULT_TPO_INTERVAL_MS as u64),
        }
    }
}

pub fn spawn_profile_engine(
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<ProfileStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = ProfileTracker::default();

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
pub struct ProfileTracker {
    config: ProfileConfig,
    session_start_ms: i64,
    current_tpo_bucket_start_ms: Option<i64>,
    current_tpo_bins: BTreeSet<i64>,
    tpo_counts: BTreeMap<i64, u64>,
    volume_by_bin: BTreeMap<i64, f64>,
    trades_seen: u64,
    sum_qty: f64,
    sum_price_qty: f64,
    sum_price2_qty: f64,
    previous_npoc: Option<NakedPocStats>,
}

impl Default for ProfileTracker {
    fn default() -> Self {
        Self::new(ProfileConfig::default())
    }
}

impl ProfileTracker {
    pub fn new(config: ProfileConfig) -> Self {
        Self {
            config,
            session_start_ms: 0,
            current_tpo_bucket_start_ms: None,
            current_tpo_bins: BTreeSet::new(),
            tpo_counts: BTreeMap::new(),
            volume_by_bin: BTreeMap::new(),
            trades_seen: 0,
            sum_qty: 0.0,
            sum_price_qty: 0.0,
            sum_price2_qty: 0.0,
            previous_npoc: None,
        }
    }

    pub fn observe_trade(&mut self, trade: TradeEvent) -> ProfileStats {
        let session_start_ms = session_start_ms(trade.ts_local_ms);
        if self.trades_seen == 0 {
            self.session_start_ms = session_start_ms;
        } else if session_start_ms != self.session_start_ms {
            self.roll_session(session_start_ms);
        }

        self.mark_npoc_touch(trade.price);
        self.trades_seen += 1;

        let price_bin = self.price_bin(trade.price);
        self.observe_tpo(price_bin, trade.ts_local_ms);
        *self.volume_by_bin.entry(price_bin).or_default() += trade.qty;
        self.sum_qty += trade.qty;
        self.sum_price_qty += trade.price * trade.qty;
        self.sum_price2_qty += trade.price * trade.price * trade.qty;

        self.stats()
    }

    fn roll_session(&mut self, next_session_start_ms: i64) {
        self.previous_npoc = self.tpo_poc().map(|poc| NakedPocStats {
            session_start_ms: self.session_start_ms,
            price_bin: self.bin_price(poc),
            touched: false,
        });

        self.session_start_ms = next_session_start_ms;
        self.current_tpo_bucket_start_ms = None;
        self.current_tpo_bins.clear();
        self.tpo_counts.clear();
        self.volume_by_bin.clear();
        self.trades_seen = 0;
        self.sum_qty = 0.0;
        self.sum_price_qty = 0.0;
        self.sum_price2_qty = 0.0;
    }

    fn observe_tpo(&mut self, price_bin: i64, ts_local_ms: i64) {
        let interval_ms = self.config.tpo_interval.as_millis() as i64;
        let bucket_start_ms = ts_local_ms - ts_local_ms.rem_euclid(interval_ms);
        if self.current_tpo_bucket_start_ms != Some(bucket_start_ms) {
            self.current_tpo_bucket_start_ms = Some(bucket_start_ms);
            self.current_tpo_bins.clear();
        }

        if self.current_tpo_bins.insert(price_bin) {
            *self.tpo_counts.entry(price_bin).or_default() += 1;
        }
    }

    fn mark_npoc_touch(&mut self, price: f64) {
        let price_bin = self.price_bin(price);
        let bin_price = self.bin_price(price_bin);
        if let Some(npoc) = &mut self.previous_npoc
            && (npoc.price_bin - bin_price).abs() <= f64::EPSILON
        {
            npoc.touched = true;
        }
    }

    fn stats(&self) -> ProfileStats {
        let tpo = self.tpo_stats();
        ProfileStats {
            session_start_ms: self.session_start_ms,
            trades_seen: self.trades_seen,
            price_bin_size: self.config.price_bin_size,
            tpo_interval_ms: self.config.tpo_interval.as_millis() as i64,
            tpo,
            volume_profile: self.volume_profile_stats(),
            vwap: self.vwap_stats(),
            previous_npoc: self.previous_npoc.clone(),
        }
    }

    fn tpo_stats(&self) -> TpoStats {
        let total_tpos = self.tpo_counts.values().sum::<u64>();
        let poc = self.tpo_poc();
        let (value_area_low, value_area_high) = poc
            .and_then(|poc| self.value_area(poc))
            .map(|(low, high)| (Some(self.bin_price(low)), Some(self.bin_price(high))))
            .unwrap_or((None, None));

        TpoStats {
            total_tpos,
            poc: poc.map(|poc| self.bin_price(poc)),
            value_area_high,
            value_area_low,
        }
    }

    fn tpo_poc(&self) -> Option<i64> {
        self.tpo_counts
            .iter()
            .max_by(|(left_bin, left_count), (right_bin, right_count)| {
                left_count
                    .cmp(right_count)
                    .then_with(|| right_bin.cmp(left_bin))
            })
            .map(|(bin, _)| *bin)
    }

    fn value_area(&self, poc: i64) -> Option<(i64, i64)> {
        let bins = self.tpo_counts.keys().copied().collect::<Vec<_>>();
        let poc_index = bins.iter().position(|bin| *bin == poc)?;
        let target = (self.tpo_counts.values().sum::<u64>() as f64 * VALUE_AREA_PCT).ceil() as u64;
        let mut included = *self.tpo_counts.get(&poc)?;
        let mut left = poc_index;
        let mut right = poc_index;

        while included < target && (left > 0 || right + 1 < bins.len()) {
            let left_count = left
                .checked_sub(1)
                .and_then(|idx| self.tpo_counts.get(&bins[idx]).copied())
                .unwrap_or(0);
            let right_count = if right + 1 < bins.len() {
                self.tpo_counts.get(&bins[right + 1]).copied().unwrap_or(0)
            } else {
                0
            };

            if right_count >= left_count && right + 1 < bins.len() {
                right += 1;
                included += right_count;
            } else if left > 0 {
                left -= 1;
                included += left_count;
            } else {
                break;
            }
        }

        Some((bins[left], bins[right]))
    }

    fn volume_profile_stats(&self) -> VolumeProfileStats {
        let total_qty = self.volume_by_bin.values().sum::<f64>();
        let vpoc = self
            .volume_by_bin
            .iter()
            .max_by(|(left_bin, left_qty), (right_bin, right_qty)| {
                left_qty
                    .total_cmp(right_qty)
                    .then_with(|| right_bin.cmp(left_bin))
            })
            .map(|(bin, _)| self.bin_price(*bin));
        let nodes = self.volume_nodes();

        VolumeProfileStats {
            total_qty,
            vpoc,
            hvn: nodes
                .iter()
                .filter(|node| node.z_score >= 0.5)
                .cloned()
                .collect(),
            lvn: nodes
                .iter()
                .filter(|node| node.z_score <= -1.0 && self.has_neighbor_volume(node.price_bin))
                .cloned()
                .collect(),
        }
    }

    fn volume_nodes(&self) -> Vec<VolumeNodeStats> {
        if self.volume_by_bin.is_empty() {
            return Vec::new();
        }

        let mean = self.volume_by_bin.values().sum::<f64>() / self.volume_by_bin.len() as f64;
        let variance = self
            .volume_by_bin
            .values()
            .map(|qty| (qty - mean).powi(2))
            .sum::<f64>()
            / self.volume_by_bin.len() as f64;
        let std = variance.sqrt();

        self.volume_by_bin
            .iter()
            .map(|(bin, qty)| VolumeNodeStats {
                price_bin: self.bin_price(*bin),
                qty: *qty,
                z_score: if std <= f64::EPSILON {
                    0.0
                } else {
                    (qty - mean) / std
                },
            })
            .collect()
    }

    fn has_neighbor_volume(&self, price_bin: f64) -> bool {
        let bin = self.price_bin(price_bin);
        self.volume_by_bin.contains_key(&(bin - 1)) && self.volume_by_bin.contains_key(&(bin + 1))
    }

    fn vwap_stats(&self) -> VwapStats {
        if self.sum_qty <= f64::EPSILON {
            return VwapStats::default();
        }

        let vwap = self.sum_price_qty / self.sum_qty;
        let variance = (self.sum_price2_qty / self.sum_qty - vwap.powi(2)).max(0.0);
        let sigma = variance.sqrt();

        VwapStats {
            value: Some(vwap),
            sigma: Some(sigma),
            upper_1: Some(vwap + sigma),
            lower_1: Some(vwap - sigma),
            upper_2: Some(vwap + 2.0 * sigma),
            lower_2: Some(vwap - 2.0 * sigma),
        }
    }

    fn price_bin(&self, price: f64) -> i64 {
        (price / self.config.price_bin_size).floor() as i64
    }

    fn bin_price(&self, price_bin: i64) -> f64 {
        price_bin as f64 * self.config.price_bin_size
    }
}

fn session_start_ms(ts_local_ms: i64) -> i64 {
    ts_local_ms - ts_local_ms.rem_euclid(SESSION_MS)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{AggressorSide, Exchange};

    #[test]
    fn computes_vwap_and_weighted_sigma() {
        let mut tracker = ProfileTracker::default();
        tracker.observe_trade(trade(1, 100.0, 1.0));
        let stats = tracker.observe_trade(trade(2, 102.0, 1.0));

        assert_eq!(stats.vwap.value, Some(101.0));
        assert_eq!(stats.vwap.sigma, Some(1.0));
        assert_eq!(stats.vwap.upper_2, Some(103.0));
        assert_eq!(stats.vwap.lower_2, Some(99.0));
    }

    #[test]
    fn counts_tpo_once_per_price_bin_per_interval() {
        let mut tracker = ProfileTracker::new(ProfileConfig {
            price_bin_size: 1.0,
            tpo_interval: Duration::from_millis(1_000),
        });
        tracker.observe_trade(trade(100, 100.2, 1.0));
        tracker.observe_trade(trade(200, 100.8, 1.0));
        let stats = tracker.observe_trade(trade(1_100, 100.1, 1.0));

        assert_eq!(stats.tpo.total_tpos, 2);
        assert_eq!(stats.tpo.poc, Some(100.0));
    }

    #[test]
    fn builds_value_area_around_tpo_poc() {
        let mut tracker = ProfileTracker::new(ProfileConfig {
            price_bin_size: 1.0,
            tpo_interval: Duration::from_millis(1_000),
        });

        tracker.observe_trade(trade(100, 99.0, 1.0));
        tracker.observe_trade(trade(1_100, 100.0, 1.0));
        tracker.observe_trade(trade(2_100, 100.0, 1.0));
        tracker.observe_trade(trade(3_100, 101.0, 1.0));
        let stats = tracker.observe_trade(trade(4_100, 102.0, 1.0));

        assert_eq!(stats.tpo.poc, Some(100.0));
        assert_eq!(stats.tpo.value_area_low, Some(100.0));
        assert_eq!(stats.tpo.value_area_high, Some(102.0));
    }

    #[test]
    fn finds_vpoc_hvn_and_lvn() {
        let mut tracker = ProfileTracker::default();
        tracker.observe_trade(trade(1, 99.0, 5.0));
        tracker.observe_trade(trade(2, 100.0, 1.0));
        let stats = tracker.observe_trade(trade(3, 101.0, 5.0));

        assert_eq!(stats.volume_profile.vpoc, Some(99.0));
        assert_eq!(stats.volume_profile.hvn.len(), 2);
        assert_eq!(stats.volume_profile.lvn[0].price_bin, 100.0);
    }

    #[test]
    fn carries_previous_session_npoc_until_touched() {
        let mut tracker = ProfileTracker::default();
        tracker.observe_trade(trade(1, 100.0, 1.0));
        let next_session = SESSION_MS + 1;
        let stats = tracker.observe_trade(trade(next_session, 101.0, 1.0));

        assert_eq!(
            stats.previous_npoc.as_ref().map(|npoc| npoc.price_bin),
            Some(100.0)
        );
        assert_eq!(
            stats.previous_npoc.as_ref().map(|npoc| npoc.touched),
            Some(false)
        );

        let touched = tracker.observe_trade(trade(next_session + 1, 100.0, 1.0));
        assert_eq!(
            touched.previous_npoc.as_ref().map(|npoc| npoc.touched),
            Some(true)
        );
    }

    fn trade(ts_local_ms: i64, price: f64, qty: f64) -> TradeEvent {
        TradeEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            price,
            qty,
            aggressor_side: AggressorSide::Buy,
        }
    }
}
