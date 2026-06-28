use crate::events::{MarketEvent, OptionGreekEvent, OptionType};
use serde::Serialize;
use std::{cmp::Ordering, collections::BTreeMap, time::Duration};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

const DEFAULT_STALE_AFTER_MS: i64 = 180_000;
const DEFAULT_THRESHOLD_USD: f64 = 1_000_000.0;
const VACUUM_RATIO: f64 = 0.10;

#[derive(Debug, Clone, Default, Serialize)]
pub struct GexStats {
    pub options_seen: u64,
    pub spot_price: Option<f64>,
    pub total_gex_1pct_usd: f64,
    pub regime: GexRegime,
    pub stale: bool,
    pub snapshot_age_ms: Option<i64>,
    pub gamma_flip: Option<f64>,
    pub max_gex_wall: Option<GexLevelStats>,
    pub max_neg_gex: Option<GexLevelStats>,
    pub gamma_vacuum: Option<GammaVacuumStats>,
    pub strikes: Vec<GexLevelStats>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GexRegime {
    Positive,
    Negative,
    #[default]
    Neutral,
}

#[derive(Debug, Clone, Serialize)]
pub struct GexLevelStats {
    pub strike: f64,
    pub expiry_ms: i64,
    pub gex_1pct_usd: f64,
    pub call_gex_1pct_usd: f64,
    pub put_gex_1pct_usd: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct GammaVacuumStats {
    pub low_strike: f64,
    pub high_strike: f64,
    pub concentration_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct GexConfig {
    pub default_call_sign: f64,
    pub default_put_sign: f64,
    pub threshold_usd: f64,
    pub stale_after: Duration,
}

impl Default for GexConfig {
    fn default() -> Self {
        Self {
            default_call_sign: -1.0,
            default_put_sign: -1.0,
            threshold_usd: DEFAULT_THRESHOLD_USD,
            stale_after: Duration::from_millis(DEFAULT_STALE_AFTER_MS as u64),
        }
    }
}

pub fn spawn_gex_engine(
    mut market_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<GexStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tracker = GexTracker::default();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    match event {
                        MarketEvent::OptionGreek(greek) => {
                            let stats = tracker.observe_option(greek);
                            let _ = stats_tx.send(stats);
                        }
                        MarketEvent::Ticker(ticker) => {
                            let spot = ticker.mark.unwrap_or((ticker.bid + ticker.ask) / 2.0);
                            let stats = tracker.observe_spot(ticker.ts_local_ms, spot);
                            let _ = stats_tx.send(stats);
                        }
                        MarketEvent::Trade(trade) => {
                            let stats = tracker.observe_spot(trade.ts_local_ms, trade.price);
                            let _ = stats_tx.send(stats);
                        }
                        _ => {}
                    }
                }
            }
        }
    })
}

#[derive(Debug, Clone)]
pub struct GexTracker {
    config: GexConfig,
    spot_price: Option<f64>,
    last_ts_local_ms: Option<i64>,
    options: BTreeMap<OptionKey, OptionGreekEvent>,
    options_seen: u64,
}

impl Default for GexTracker {
    fn default() -> Self {
        Self::new(GexConfig::default())
    }
}

impl GexTracker {
    pub fn new(config: GexConfig) -> Self {
        Self {
            config,
            spot_price: None,
            last_ts_local_ms: None,
            options: BTreeMap::new(),
            options_seen: 0,
        }
    }

    pub fn observe_spot(&mut self, ts_local_ms: i64, spot_price: f64) -> GexStats {
        self.spot_price = Some(spot_price);
        self.last_ts_local_ms = Some(ts_local_ms);
        self.stats()
    }

    pub fn observe_option(&mut self, greek: OptionGreekEvent) -> GexStats {
        self.options_seen += 1;
        self.last_ts_local_ms = Some(greek.ts_local_ms);
        self.options.insert(OptionKey::from(&greek), greek);
        self.stats()
    }

    fn stats(&self) -> GexStats {
        let strikes = self.strike_stats();
        let total_gex_1pct_usd = strikes.iter().map(|level| level.gex_1pct_usd).sum::<f64>();
        let regime = if total_gex_1pct_usd > self.config.threshold_usd {
            GexRegime::Positive
        } else if total_gex_1pct_usd < -self.config.threshold_usd {
            GexRegime::Negative
        } else {
            GexRegime::Neutral
        };
        let snapshot_age_ms = self
            .last_ts_local_ms
            .zip(self.options.values().map(|event| event.ts_local_ms).max())
            .map(|(now, snapshot)| now.saturating_sub(snapshot));
        let stale = snapshot_age_ms
            .map(|age| age > self.config.stale_after.as_millis() as i64)
            .unwrap_or(true);

        GexStats {
            options_seen: self.options_seen,
            spot_price: self.spot_price,
            total_gex_1pct_usd,
            regime,
            stale,
            snapshot_age_ms,
            gamma_flip: gamma_flip(&strikes),
            max_gex_wall: strikes
                .iter()
                .filter(|level| level.gex_1pct_usd > 0.0)
                .max_by(|left, right| left.gex_1pct_usd.total_cmp(&right.gex_1pct_usd))
                .cloned(),
            max_neg_gex: strikes
                .iter()
                .filter(|level| level.gex_1pct_usd < 0.0)
                .min_by(|left, right| left.gex_1pct_usd.total_cmp(&right.gex_1pct_usd))
                .cloned(),
            gamma_vacuum: gamma_vacuum(&strikes),
            strikes,
        }
    }

    fn strike_stats(&self) -> Vec<GexLevelStats> {
        let Some(spot_price) = self.spot_price else {
            return Vec::new();
        };

        let mut by_strike_expiry = BTreeMap::<StrikeExpiryKey, GexLevelStats>::new();
        for option in self.options.values() {
            let dealer_sign = match option.option_type {
                OptionType::Call => self.config.default_call_sign,
                OptionType::Put => self.config.default_put_sign,
            };
            let gex = option.gamma
                * option.open_interest_contracts
                * option.contract_unit
                * spot_price.powi(2)
                * 0.01
                * dealer_sign;
            let key = StrikeExpiryKey {
                strike: PriceKey(option.strike),
                expiry_ms: option.expiry_ms,
            };
            let level = by_strike_expiry.entry(key).or_insert(GexLevelStats {
                strike: option.strike,
                expiry_ms: option.expiry_ms,
                gex_1pct_usd: 0.0,
                call_gex_1pct_usd: 0.0,
                put_gex_1pct_usd: 0.0,
            });

            level.gex_1pct_usd += gex;
            match option.option_type {
                OptionType::Call => level.call_gex_1pct_usd += gex,
                OptionType::Put => level.put_gex_1pct_usd += gex,
            }
        }

        by_strike_expiry.into_values().collect()
    }
}

fn gamma_flip(strikes: &[GexLevelStats]) -> Option<f64> {
    let mut cumulative = 0.0;
    let mut previous: Option<(f64, f64)> = None;

    for level in strikes {
        cumulative += level.gex_1pct_usd;
        if let Some((previous_strike, previous_cumulative)) = previous
            && previous_cumulative.signum() != cumulative.signum()
        {
            return Some((previous_strike + level.strike) / 2.0);
        }
        previous = Some((level.strike, cumulative));
    }

    None
}

fn gamma_vacuum(strikes: &[GexLevelStats]) -> Option<GammaVacuumStats> {
    let max_abs = strikes
        .iter()
        .map(|level| level.gex_1pct_usd.abs())
        .fold(0.0, f64::max);
    if max_abs <= f64::EPSILON || strikes.len() < 2 {
        return None;
    }

    strikes
        .windows(2)
        .filter_map(|pair| {
            let left = &pair[0];
            let right = &pair[1];
            let concentration =
                (left.gex_1pct_usd.abs() + right.gex_1pct_usd.abs()) / (2.0 * max_abs);
            (concentration <= VACUUM_RATIO).then_some(GammaVacuumStats {
                low_strike: left.strike,
                high_strike: right.strike,
                concentration_ratio: concentration,
            })
        })
        .min_by(|left, right| {
            left.concentration_ratio
                .total_cmp(&right.concentration_ratio)
        })
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct OptionKey {
    expiry_ms: i64,
    strike: PriceKey,
    option_type_rank: u8,
    symbol_hash: u64,
}

impl OptionKey {
    fn from(option: &OptionGreekEvent) -> Self {
        Self {
            expiry_ms: option.expiry_ms,
            strike: PriceKey(option.strike),
            option_type_rank: match option.option_type {
                OptionType::Call => 0,
                OptionType::Put => 1,
            },
            symbol_hash: stable_hash(&option.option_symbol),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct StrikeExpiryKey {
    strike: PriceKey,
    expiry_ms: i64,
}

fn stable_hash(value: &str) -> u64 {
    value
        .bytes()
        .fold(14_695_981_039_346_656_037, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(1_099_511_628_211)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Exchange;

    #[test]
    fn computes_gex_per_option_and_regime() {
        let mut tracker = GexTracker::new(GexConfig {
            threshold_usd: 10.0,
            default_call_sign: 1.0,
            default_put_sign: -1.0,
            ..Default::default()
        });
        tracker.observe_spot(1, 100.0);
        tracker.observe_option(option("BTC-100-C", 100.0, OptionType::Call, 2.0, 1.0));
        let stats = tracker.observe_option(option("BTC-90-P", 90.0, OptionType::Put, 1.0, 1.0));

        assert_eq!(stats.total_gex_1pct_usd, 100.0);
        assert_eq!(stats.regime, GexRegime::Positive);
        assert_eq!(
            stats.max_gex_wall.as_ref().map(|level| level.strike),
            Some(100.0)
        );
        assert_eq!(
            stats.max_neg_gex.as_ref().map(|level| level.strike),
            Some(90.0)
        );
    }

    #[test]
    fn finds_gamma_flip_between_cumulative_signs() {
        let strikes = vec![
            GexLevelStats {
                strike: 90.0,
                expiry_ms: 1,
                gex_1pct_usd: -10.0,
                call_gex_1pct_usd: 0.0,
                put_gex_1pct_usd: -10.0,
            },
            GexLevelStats {
                strike: 100.0,
                expiry_ms: 1,
                gex_1pct_usd: 20.0,
                call_gex_1pct_usd: 20.0,
                put_gex_1pct_usd: 0.0,
            },
        ];

        assert_eq!(gamma_flip(&strikes), Some(95.0));
    }

    #[test]
    fn marks_stale_after_configured_window() {
        let mut tracker = GexTracker::new(GexConfig {
            stale_after: Duration::from_millis(10),
            ..Default::default()
        });
        tracker.observe_spot(1, 100.0);
        tracker.observe_option(option("BTC-100-C", 100.0, OptionType::Call, 1.0, 1.0));
        let stats = tracker.observe_spot(100, 101.0);

        assert!(stats.stale);
        assert_eq!(stats.snapshot_age_ms, Some(99));
    }

    fn option(
        option_symbol: &str,
        strike: f64,
        option_type: OptionType,
        open_interest_contracts: f64,
        gamma: f64,
    ) -> OptionGreekEvent {
        OptionGreekEvent {
            exchange: Exchange::Binance,
            underlying: "BTCUSDT".to_string(),
            option_symbol: option_symbol.to_string(),
            expiry_ms: 86_400_000,
            strike,
            option_type,
            ts_local_ms: 1,
            mark_price: 1.0,
            open_interest_contracts,
            contract_unit: 1.0,
            gamma,
            delta: None,
            iv: None,
        }
    }
}
