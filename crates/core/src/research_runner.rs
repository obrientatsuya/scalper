use crate::{
    events::MarketEvent,
    heatmap::HeatmapTracker,
    orderflow::OrderflowTracker,
    profile::ProfileTracker,
    research::{
        CandidateLabel, FeatureSnapshot, PriceSample, ThresholdResult, WalkForwardSplit,
        generate_candidates, label_candidate, optimize_threshold, walk_forward_splits,
    },
    storage::read_events_from_parquet,
};
use serde::Serialize;
use std::path::Path;

#[derive(Debug, Clone, Serialize)]
pub struct ResearchRunReport {
    pub events_read: usize,
    pub event_counts: ResearchEventCounts,
    pub price_samples: usize,
    pub candidates: usize,
    pub labels: Vec<CandidateLabel>,
    pub walk_forward_splits: Vec<WalkForwardSplit>,
    pub best_threshold: Option<ThresholdResult>,
    pub paper_validation: Option<PaperValidationStats>,
    pub walk_forward_validation: Option<WalkForwardValidationStats>,
    pub data_quality: ResearchDataQuality,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegimeScanReport {
    pub events_read: usize,
    pub price_samples: usize,
    pub window_size: usize,
    pub step_size: usize,
    pub windows: usize,
    pub observed_trend_regimes: Vec<TrendRegime>,
    pub observed_volatility_regimes: Vec<VolatilityRegime>,
    pub regime_results: Vec<RegimeScanResult>,
    pub window_results: Vec<RegimeScanWindow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegimeScanResult {
    pub trend: TrendRegime,
    pub volatility: VolatilityRegime,
    pub windows: usize,
    pub avg_return_pct: f64,
    pub avg_realized_vol_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RegimeScanWindow {
    pub start_ts_ms: i64,
    pub end_ts_ms: i64,
    pub samples: usize,
    pub regime: SplitRegime,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ResearchEventCounts {
    pub trades: usize,
    pub tickers: usize,
    pub klines: usize,
    pub depth_deltas: usize,
    pub orderbook_snapshots: usize,
    pub option_greeks: usize,
    pub heartbeats: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct PaperValidationStats {
    pub threshold: f64,
    pub trades: usize,
    pub win_rate: f64,
    pub expectancy_r: f64,
    pub total_r: f64,
    pub max_drawdown_r: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResearchDataQuality {
    pub sufficient: bool,
    pub min_trade_events: usize,
    pub min_paper_trades: usize,
    pub min_walk_forward_splits: usize,
    pub min_oos_trades: usize,
    pub min_oos_expectancy_r: f64,
    pub min_positive_oos_split_rate: f64,
    pub min_trend_regimes: usize,
    pub min_volatility_regimes: usize,
    pub trade_events: usize,
    pub paper_trades: usize,
    pub walk_forward_splits: usize,
    pub oos_trades: usize,
    pub oos_expectancy_r: Option<f64>,
    pub positive_oos_split_rate: Option<f64>,
    pub observed_trend_regimes: Vec<TrendRegime>,
    pub observed_volatility_regimes: Vec<VolatilityRegime>,
    pub issues: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardValidationStats {
    pub splits: usize,
    pub evaluated_splits: usize,
    pub positive_splits: usize,
    pub positive_split_rate: f64,
    pub trades: usize,
    pub win_rate: f64,
    pub expectancy_r: f64,
    pub total_r: f64,
    pub max_drawdown_r: f64,
    pub observed_trend_regimes: Vec<TrendRegime>,
    pub observed_volatility_regimes: Vec<VolatilityRegime>,
    pub regime_results: Vec<WalkForwardRegimeResult>,
    pub split_results: Vec<WalkForwardSplitResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardSplitResult {
    pub train_start: usize,
    pub train_end: usize,
    pub test_start: usize,
    pub test_end: usize,
    pub threshold: f64,
    pub train_expectancy_r: f64,
    pub test_trades: usize,
    pub test_expectancy_r: f64,
    pub test_total_r: f64,
    pub regime: SplitRegime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TrendRegime {
    Uptrend,
    Downtrend,
    Range,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VolatilityRegime {
    Low,
    Normal,
    High,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct SplitRegime {
    pub trend: TrendRegime,
    pub volatility: VolatilityRegime,
    pub start_price: f64,
    pub end_price: f64,
    pub return_pct: f64,
    pub realized_vol_pct: f64,
}

impl Default for SplitRegime {
    fn default() -> Self {
        Self {
            trend: TrendRegime::Range,
            volatility: VolatilityRegime::Low,
            start_price: 0.0,
            end_price: 0.0,
            return_pct: 0.0,
            realized_vol_pct: 0.0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardRegimeResult {
    pub trend: TrendRegime,
    pub volatility: VolatilityRegime,
    pub splits: usize,
    pub positive_splits: usize,
    pub positive_split_rate: f64,
    pub trades: usize,
    pub expectancy_r: f64,
    pub total_r: f64,
}

#[derive(Debug, Clone)]
pub struct ResearchRunConfig {
    pub train_size: usize,
    pub test_size: usize,
    pub thresholds: Vec<f64>,
    pub min_snapshot_interval_ms: i64,
    pub min_trade_events: usize,
    pub min_paper_trades: usize,
    pub min_walk_forward_splits: usize,
    pub min_oos_trades: usize,
    pub min_oos_expectancy_r: f64,
    pub min_positive_oos_split_rate: f64,
    pub min_trend_regimes: usize,
    pub min_volatility_regimes: usize,
}

impl Default for ResearchRunConfig {
    fn default() -> Self {
        Self {
            train_size: 500,
            test_size: 100,
            thresholds: vec![0.0, 1.0, 2.0, 3.0],
            min_snapshot_interval_ms: 0,
            min_trade_events: 500,
            min_paper_trades: 100,
            min_walk_forward_splits: 1,
            min_oos_trades: 50,
            min_oos_expectancy_r: 0.0,
            min_positive_oos_split_rate: 0.60,
            min_trend_regimes: 3,
            min_volatility_regimes: 3,
        }
    }
}

pub fn run_research_from_parquet(
    input_path: impl AsRef<Path>,
    config: ResearchRunConfig,
) -> anyhow::Result<ResearchRunReport> {
    let events = read_events_from_parquet(input_path)?;
    Ok(run_research(events, config))
}

pub fn scan_regimes_from_parquet(
    input_path: impl AsRef<Path>,
    window_size: usize,
    step_size: usize,
) -> anyhow::Result<RegimeScanReport> {
    let events = read_events_from_parquet(input_path)?;
    Ok(scan_regimes(events, window_size, step_size))
}

pub fn scan_regimes(
    events: Vec<MarketEvent>,
    window_size: usize,
    step_size: usize,
) -> RegimeScanReport {
    let events_read = events.len();
    let price_samples = events
        .into_iter()
        .filter_map(price_sample_from_event)
        .collect::<Vec<_>>();
    let window_size = window_size.max(2);
    let step_size = step_size.max(1);
    let mut window_results = Vec::new();
    let mut start = 0;

    while start + window_size <= price_samples.len() {
        let window = &price_samples[start..start + window_size];
        window_results.push(RegimeScanWindow {
            start_ts_ms: window
                .first()
                .map(|sample| sample.ts_local_ms)
                .unwrap_or_default(),
            end_ts_ms: window
                .last()
                .map(|sample| sample.ts_local_ms)
                .unwrap_or_default(),
            samples: window.len(),
            regime: classify_price_samples(window),
        });
        start += step_size;
    }

    let observed_trend_regimes = observed_scan_trend_regimes(&window_results);
    let observed_volatility_regimes = observed_scan_volatility_regimes(&window_results);
    let regime_results = scan_regime_results(&window_results);

    RegimeScanReport {
        events_read,
        price_samples: price_samples.len(),
        window_size,
        step_size,
        windows: window_results.len(),
        observed_trend_regimes,
        observed_volatility_regimes,
        regime_results,
        window_results,
    }
}

pub fn run_research(events: Vec<MarketEvent>, config: ResearchRunConfig) -> ResearchRunReport {
    let events_read = events.len();
    let mut event_counts = ResearchEventCounts::default();
    let mut feature_snapshots = Vec::new();
    let mut price_samples = Vec::new();

    let mut orderflow = OrderflowTracker::default();
    let mut profile = ProfileTracker::default();
    let mut heatmap = HeatmapTracker::default();

    let mut last_obi_20 = None;
    let mut last_feature_ts_ms = None;

    for event in events {
        match event {
            MarketEvent::Trade(trade) => {
                event_counts.trades += 1;
                let ts_local_ms = trade.ts_local_ms;
                let price = trade.price;

                let orderflow_stats = orderflow.observe_trade(trade.clone());
                let delta_z_1s = orderflow_stats
                    .windows
                    .iter()
                    .find(|window| window.window_ms == 1_000)
                    .and_then(|window| window.delta_z);

                let profile_stats = profile.observe_trade(trade);

                let rolling = rolling_features(&price_samples, price);
                price_samples.push(PriceSample { ts_local_ms, price });
                if should_emit_feature_snapshot(
                    ts_local_ms,
                    config.min_snapshot_interval_ms,
                    &mut last_feature_ts_ms,
                ) {
                    feature_snapshots.push(FeatureSnapshot {
                        ts_local_ms,
                        price,
                        vwap: profile_stats.vwap.value.or(rolling.vwap),
                        vwap_upper_2: profile_stats.vwap.upper_2.or(rolling.vwap_upper_2),
                        vwap_lower_2: profile_stats.vwap.lower_2.or(rolling.vwap_lower_2),
                        value_area_high: profile_stats
                            .tpo
                            .value_area_high
                            .or(rolling.value_area_high),
                        value_area_low: profile_stats.tpo.value_area_low.or(rolling.value_area_low),
                        delta_z_1s: delta_z_1s.or(rolling.delta_z_1s),
                        obi_20: last_obi_20,
                    });
                }
            }
            MarketEvent::Ticker(ticker) => {
                event_counts.tickers += 1;
                let price = ticker.mark.unwrap_or((ticker.bid + ticker.ask) / 2.0);
                let rolling = rolling_features(&price_samples, price);
                price_samples.push(PriceSample {
                    ts_local_ms: ticker.ts_local_ms,
                    price,
                });
                if should_emit_feature_snapshot(
                    ticker.ts_local_ms,
                    config.min_snapshot_interval_ms,
                    &mut last_feature_ts_ms,
                ) {
                    feature_snapshots.push(FeatureSnapshot {
                        ts_local_ms: ticker.ts_local_ms,
                        price,
                        vwap: rolling.vwap,
                        vwap_upper_2: rolling.vwap_upper_2,
                        vwap_lower_2: rolling.vwap_lower_2,
                        value_area_high: rolling.value_area_high,
                        value_area_low: rolling.value_area_low,
                        delta_z_1s: rolling.delta_z_1s,
                        obi_20: last_obi_20,
                    });
                }
            }
            MarketEvent::Kline(kline) => {
                event_counts.klines += 1;
                let price = kline.close;
                let rolling = rolling_features(&price_samples, price);
                price_samples.push(PriceSample {
                    ts_local_ms: kline.close_time_ms,
                    price,
                });
                if should_emit_feature_snapshot(
                    kline.close_time_ms,
                    config.min_snapshot_interval_ms,
                    &mut last_feature_ts_ms,
                ) {
                    feature_snapshots.push(FeatureSnapshot {
                        ts_local_ms: kline.close_time_ms,
                        price,
                        vwap: rolling.vwap,
                        vwap_upper_2: rolling.vwap_upper_2,
                        vwap_lower_2: rolling.vwap_lower_2,
                        value_area_high: rolling.value_area_high,
                        value_area_low: rolling.value_area_low,
                        delta_z_1s: rolling.delta_z_1s,
                        obi_20: last_obi_20,
                    });
                }
            }
            MarketEvent::DepthDelta(delta) => {
                event_counts.depth_deltas += 1;
                let heatmap_stats = heatmap.apply_delta(delta);
                last_obi_20 = heatmap_stats.obi_20;
            }
            MarketEvent::OrderBookSnapshot(snapshot) => {
                event_counts.orderbook_snapshots += 1;
                let heatmap_stats = heatmap.apply_snapshot(snapshot);
                last_obi_20 = heatmap_stats.obi_20;
            }
            MarketEvent::OptionGreek(_) => {
                event_counts.option_greeks += 1;
            }
            MarketEvent::Heartbeat { .. } => {
                event_counts.heartbeats += 1;
            }
        }
    }

    let mut labels = Vec::new();
    for snapshot in feature_snapshots {
        let future_start =
            price_samples.partition_point(|sample| sample.ts_local_ms <= snapshot.ts_local_ms);
        let future = &price_samples[future_start..];

        for candidate in generate_candidates(&snapshot) {
            labels.push(label_candidate(candidate, future));
        }
    }

    let best_threshold = optimize_threshold(&labels, &config.thresholds);
    let paper_validation = best_threshold
        .as_ref()
        .and_then(|threshold| paper_validate(&labels, threshold.threshold));
    let walk_forward_splits =
        walk_forward_splits(labels.len(), config.train_size, config.test_size);
    let walk_forward_validation = walk_forward_validate(
        &labels,
        &price_samples,
        &walk_forward_splits,
        &config.thresholds,
    );
    let data_quality = data_quality(
        &event_counts,
        paper_validation.as_ref(),
        walk_forward_validation.as_ref(),
        walk_forward_splits.len(),
        &config,
    );

    ResearchRunReport {
        events_read,
        event_counts,
        price_samples: price_samples.len(),
        candidates: labels.len(),
        labels,
        walk_forward_splits,
        best_threshold,
        paper_validation,
        walk_forward_validation,
        data_quality,
    }
}

fn data_quality(
    event_counts: &ResearchEventCounts,
    paper_validation: Option<&PaperValidationStats>,
    walk_forward_validation: Option<&WalkForwardValidationStats>,
    walk_forward_splits: usize,
    config: &ResearchRunConfig,
) -> ResearchDataQuality {
    let paper_trades = paper_validation
        .map(|stats| stats.trades)
        .unwrap_or_default();
    let oos_trades = walk_forward_validation
        .map(|stats| stats.trades)
        .unwrap_or_default();
    let oos_expectancy_r = walk_forward_validation.map(|stats| stats.expectancy_r);
    let positive_oos_split_rate = walk_forward_validation.map(|stats| stats.positive_split_rate);
    let observed_trend_regimes = walk_forward_validation
        .map(|stats| stats.observed_trend_regimes.clone())
        .unwrap_or_default();
    let observed_volatility_regimes = walk_forward_validation
        .map(|stats| stats.observed_volatility_regimes.clone())
        .unwrap_or_default();
    let mut issues = Vec::new();

    if event_counts.trades < config.min_trade_events {
        issues.push(format!(
            "trade_events {} below min {}",
            event_counts.trades, config.min_trade_events
        ));
    }
    if paper_trades < config.min_paper_trades {
        issues.push(format!(
            "paper_trades {paper_trades} below min {}",
            config.min_paper_trades
        ));
    }
    if walk_forward_splits < config.min_walk_forward_splits {
        issues.push(format!(
            "walk_forward_splits {walk_forward_splits} below min {}",
            config.min_walk_forward_splits
        ));
    }
    if oos_trades < config.min_oos_trades {
        issues.push(format!(
            "oos_trades {oos_trades} below min {}",
            config.min_oos_trades
        ));
    }
    match oos_expectancy_r {
        Some(expectancy) if expectancy >= config.min_oos_expectancy_r => {}
        Some(expectancy) => issues.push(format!(
            "oos_expectancy_r {expectancy:.6} below min {:.6}",
            config.min_oos_expectancy_r
        )),
        None => issues.push("oos_expectancy_r missing".to_string()),
    }
    match positive_oos_split_rate {
        Some(rate) if rate >= config.min_positive_oos_split_rate => {}
        Some(rate) => issues.push(format!(
            "positive_oos_split_rate {rate:.6} below min {:.6}",
            config.min_positive_oos_split_rate
        )),
        None => issues.push("positive_oos_split_rate missing".to_string()),
    }
    if observed_trend_regimes.len() < config.min_trend_regimes {
        issues.push(format!(
            "trend_regimes {} below min {}",
            observed_trend_regimes.len(),
            config.min_trend_regimes
        ));
    }
    if observed_volatility_regimes.len() < config.min_volatility_regimes {
        issues.push(format!(
            "volatility_regimes {} below min {}",
            observed_volatility_regimes.len(),
            config.min_volatility_regimes
        ));
    }

    ResearchDataQuality {
        sufficient: issues.is_empty(),
        min_trade_events: config.min_trade_events,
        min_paper_trades: config.min_paper_trades,
        min_walk_forward_splits: config.min_walk_forward_splits,
        min_oos_trades: config.min_oos_trades,
        min_oos_expectancy_r: config.min_oos_expectancy_r,
        min_positive_oos_split_rate: config.min_positive_oos_split_rate,
        min_trend_regimes: config.min_trend_regimes,
        min_volatility_regimes: config.min_volatility_regimes,
        trade_events: event_counts.trades,
        paper_trades,
        walk_forward_splits,
        oos_trades,
        oos_expectancy_r,
        positive_oos_split_rate,
        observed_trend_regimes,
        observed_volatility_regimes,
        issues,
    }
}

fn should_emit_feature_snapshot(
    ts_local_ms: i64,
    min_interval_ms: i64,
    last_feature_ts_ms: &mut Option<i64>,
) -> bool {
    if min_interval_ms <= 0
        || last_feature_ts_ms.is_none_or(|last| ts_local_ms - last >= min_interval_ms)
    {
        *last_feature_ts_ms = Some(ts_local_ms);
        true
    } else {
        false
    }
}

fn rolling_features(past: &[PriceSample], price: f64) -> FeatureSnapshot {
    let window = past.iter().rev().take(100).collect::<Vec<_>>();
    if window.len() < 5 {
        return FeatureSnapshot::default();
    }

    let mean = window.iter().map(|sample| sample.price).sum::<f64>() / window.len() as f64;
    let variance = window
        .iter()
        .map(|sample| (sample.price - mean).powi(2))
        .sum::<f64>()
        / window.len() as f64;
    let sigma = variance.sqrt();
    let value_area_high = window
        .iter()
        .map(|sample| sample.price)
        .fold(f64::MIN, f64::max);
    let value_area_low = window
        .iter()
        .map(|sample| sample.price)
        .fold(f64::MAX, f64::min);
    let previous = window.first().map(|sample| sample.price).unwrap_or(price);
    let delta_z_1s = if sigma <= f64::EPSILON {
        None
    } else {
        Some((price - previous) / sigma)
    };

    FeatureSnapshot {
        vwap: Some(mean),
        vwap_upper_2: Some(mean + 2.0 * sigma),
        vwap_lower_2: Some(mean - 2.0 * sigma),
        value_area_high: Some(value_area_high),
        value_area_low: Some(value_area_low),
        delta_z_1s,
        ..Default::default()
    }
}

pub fn paper_validate(labels: &[CandidateLabel], threshold: f64) -> Option<PaperValidationStats> {
    let returns = threshold_returns(labels, threshold);
    if returns.is_empty() {
        return None;
    }

    let mut equity = 0.0_f64;
    let mut peak = 0.0_f64;
    let mut max_drawdown_r = 0.0_f64;
    let mut wins = 0;

    for value in &returns {
        if *value > 0.0 {
            wins += 1;
        }
        equity += *value;
        peak = peak.max(equity);
        max_drawdown_r = max_drawdown_r.max(peak - equity);
    }

    Some(PaperValidationStats {
        threshold,
        trades: returns.len(),
        win_rate: wins as f64 / returns.len() as f64,
        expectancy_r: equity / returns.len() as f64,
        total_r: equity,
        max_drawdown_r,
    })
}

fn walk_forward_validate(
    labels: &[CandidateLabel],
    price_samples: &[PriceSample],
    splits: &[WalkForwardSplit],
    thresholds: &[f64],
) -> Option<WalkForwardValidationStats> {
    if splits.is_empty() {
        return None;
    }

    let mut split_results = Vec::new();
    let mut all_returns = Vec::new();

    for split in splits {
        let train = &labels[split.train_start..split.train_end];
        let test = &labels[split.test_start..split.test_end];
        let Some(best) = optimize_threshold(train, thresholds) else {
            continue;
        };
        let returns = threshold_returns(test, best.threshold);
        if returns.is_empty() {
            continue;
        }

        let test_total_r = returns.iter().sum::<f64>();
        let regime = classify_regime(test, price_samples);
        split_results.push(WalkForwardSplitResult {
            train_start: split.train_start,
            train_end: split.train_end,
            test_start: split.test_start,
            test_end: split.test_end,
            threshold: best.threshold,
            train_expectancy_r: best.expectancy_r,
            test_trades: returns.len(),
            test_expectancy_r: test_total_r / returns.len() as f64,
            test_total_r,
            regime,
        });
        all_returns.extend(returns);
    }

    if all_returns.is_empty() {
        return None;
    }

    let mut equity = 0.0_f64;
    let mut peak = 0.0_f64;
    let mut max_drawdown_r = 0.0_f64;
    let mut wins = 0;

    for value in &all_returns {
        if *value > 0.0 {
            wins += 1;
        }
        equity += *value;
        peak = peak.max(equity);
        max_drawdown_r = max_drawdown_r.max(peak - equity);
    }

    Some(WalkForwardValidationStats {
        splits: splits.len(),
        evaluated_splits: split_results.len(),
        positive_splits: split_results
            .iter()
            .filter(|result| result.test_total_r > 0.0)
            .count(),
        positive_split_rate: split_results
            .iter()
            .filter(|result| result.test_total_r > 0.0)
            .count() as f64
            / split_results.len() as f64,
        trades: all_returns.len(),
        win_rate: wins as f64 / all_returns.len() as f64,
        expectancy_r: equity / all_returns.len() as f64,
        total_r: equity,
        max_drawdown_r,
        observed_trend_regimes: observed_trend_regimes(&split_results),
        observed_volatility_regimes: observed_volatility_regimes(&split_results),
        regime_results: regime_results(&split_results),
        split_results,
    })
}

fn classify_regime(labels: &[CandidateLabel], price_samples: &[PriceSample]) -> SplitRegime {
    let Some(first_label) = labels.first() else {
        return SplitRegime::default();
    };
    let Some(last_label) = labels.last() else {
        return SplitRegime::default();
    };
    let start_ts = first_label.candidate.ts_local_ms;
    let end_ts = last_label.candidate.ts_local_ms.max(start_ts);
    let prices = price_samples
        .iter()
        .filter(|sample| sample.ts_local_ms >= start_ts && sample.ts_local_ms <= end_ts)
        .map(|sample| sample.price)
        .filter(|price| price.is_finite() && *price > 0.0)
        .collect::<Vec<_>>();

    classify_prices(prices)
}

pub fn classify_price_samples(samples: &[PriceSample]) -> SplitRegime {
    let prices = samples
        .iter()
        .map(|sample| sample.price)
        .filter(|price| price.is_finite() && *price > 0.0)
        .collect::<Vec<_>>();
    classify_prices(prices)
}

fn classify_prices(prices: Vec<f64>) -> SplitRegime {
    let start_price = prices.first().copied().unwrap_or_default();
    let end_price = prices.last().copied().unwrap_or(start_price);
    let return_pct = if start_price > 0.0 {
        (end_price - start_price) / start_price
    } else {
        0.0
    };

    let returns = prices
        .windows(2)
        .filter_map(|pair| {
            let previous = pair[0];
            let current = pair[1];
            (previous > 0.0).then_some((current - previous) / previous)
        })
        .collect::<Vec<_>>();
    let realized_vol_pct = realized_volatility(&returns);

    let trend = if return_pct > 0.001 {
        TrendRegime::Uptrend
    } else if return_pct < -0.001 {
        TrendRegime::Downtrend
    } else {
        TrendRegime::Range
    };
    let volatility = if realized_vol_pct < 0.001 {
        VolatilityRegime::Low
    } else if realized_vol_pct > 0.003 {
        VolatilityRegime::High
    } else {
        VolatilityRegime::Normal
    };

    SplitRegime {
        trend,
        volatility,
        start_price,
        end_price,
        return_pct,
        realized_vol_pct,
    }
}

fn price_sample_from_event(event: MarketEvent) -> Option<PriceSample> {
    match event {
        MarketEvent::Trade(trade) => Some(PriceSample {
            ts_local_ms: trade.ts_local_ms,
            price: trade.price,
        }),
        MarketEvent::Ticker(ticker) => Some(PriceSample {
            ts_local_ms: ticker.ts_local_ms,
            price: ticker.mark.unwrap_or((ticker.bid + ticker.ask) / 2.0),
        }),
        MarketEvent::Kline(kline) => Some(PriceSample {
            ts_local_ms: kline.close_time_ms,
            price: kline.close,
        }),
        MarketEvent::DepthDelta(_)
        | MarketEvent::OrderBookSnapshot(_)
        | MarketEvent::OptionGreek(_)
        | MarketEvent::Heartbeat { .. } => None,
    }
}

fn realized_volatility(returns: &[f64]) -> f64 {
    if returns.is_empty() {
        return 0.0;
    }

    let mean = returns.iter().sum::<f64>() / returns.len() as f64;
    let variance = returns
        .iter()
        .map(|value| (value - mean).powi(2))
        .sum::<f64>()
        / returns.len() as f64;
    variance.sqrt()
}

fn regime_results(splits: &[WalkForwardSplitResult]) -> Vec<WalkForwardRegimeResult> {
    let mut results = Vec::new();
    for trend in [
        TrendRegime::Uptrend,
        TrendRegime::Downtrend,
        TrendRegime::Range,
    ] {
        for volatility in [
            VolatilityRegime::Low,
            VolatilityRegime::Normal,
            VolatilityRegime::High,
        ] {
            let matching = splits
                .iter()
                .filter(|split| {
                    split.regime.trend == trend && split.regime.volatility == volatility
                })
                .collect::<Vec<_>>();
            if matching.is_empty() {
                continue;
            }

            let total_r = matching.iter().map(|split| split.test_total_r).sum::<f64>();
            let trades = matching
                .iter()
                .map(|split| split.test_trades)
                .sum::<usize>();
            let positive_splits = matching
                .iter()
                .filter(|split| split.test_total_r > 0.0)
                .count();

            results.push(WalkForwardRegimeResult {
                trend,
                volatility,
                splits: matching.len(),
                positive_splits,
                positive_split_rate: positive_splits as f64 / matching.len() as f64,
                trades,
                expectancy_r: if trades == 0 {
                    0.0
                } else {
                    total_r / trades as f64
                },
                total_r,
            });
        }
    }
    results
}

fn observed_trend_regimes(splits: &[WalkForwardSplitResult]) -> Vec<TrendRegime> {
    [
        TrendRegime::Uptrend,
        TrendRegime::Downtrend,
        TrendRegime::Range,
    ]
    .into_iter()
    .filter(|trend| splits.iter().any(|split| split.regime.trend == *trend))
    .collect()
}

fn observed_volatility_regimes(splits: &[WalkForwardSplitResult]) -> Vec<VolatilityRegime> {
    [
        VolatilityRegime::Low,
        VolatilityRegime::Normal,
        VolatilityRegime::High,
    ]
    .into_iter()
    .filter(|volatility| {
        splits
            .iter()
            .any(|split| split.regime.volatility == *volatility)
    })
    .collect()
}

fn observed_scan_trend_regimes(windows: &[RegimeScanWindow]) -> Vec<TrendRegime> {
    [
        TrendRegime::Uptrend,
        TrendRegime::Downtrend,
        TrendRegime::Range,
    ]
    .into_iter()
    .filter(|trend| windows.iter().any(|window| window.regime.trend == *trend))
    .collect()
}

fn observed_scan_volatility_regimes(windows: &[RegimeScanWindow]) -> Vec<VolatilityRegime> {
    [
        VolatilityRegime::Low,
        VolatilityRegime::Normal,
        VolatilityRegime::High,
    ]
    .into_iter()
    .filter(|volatility| {
        windows
            .iter()
            .any(|window| window.regime.volatility == *volatility)
    })
    .collect()
}

fn scan_regime_results(windows: &[RegimeScanWindow]) -> Vec<RegimeScanResult> {
    let mut results = Vec::new();
    for trend in [
        TrendRegime::Uptrend,
        TrendRegime::Downtrend,
        TrendRegime::Range,
    ] {
        for volatility in [
            VolatilityRegime::Low,
            VolatilityRegime::Normal,
            VolatilityRegime::High,
        ] {
            let matching = windows
                .iter()
                .filter(|window| {
                    window.regime.trend == trend && window.regime.volatility == volatility
                })
                .collect::<Vec<_>>();
            if matching.is_empty() {
                continue;
            }

            results.push(RegimeScanResult {
                trend,
                volatility,
                windows: matching.len(),
                avg_return_pct: matching
                    .iter()
                    .map(|window| window.regime.return_pct)
                    .sum::<f64>()
                    / matching.len() as f64,
                avg_realized_vol_pct: matching
                    .iter()
                    .map(|window| window.regime.realized_vol_pct)
                    .sum::<f64>()
                    / matching.len() as f64,
            });
        }
    }
    results
}

fn threshold_returns(labels: &[CandidateLabel], threshold: f64) -> Vec<f64> {
    labels
        .iter()
        .filter(|label| label.candidate.score >= threshold)
        .filter_map(|label| label.return_r_30s)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{AggressorSide, DepthDeltaEvent, DepthLevel, Exchange, TradeEvent};

    #[test]
    fn runs_research_without_lookahead() {
        let events = vec![
            MarketEvent::DepthDelta(depth(1, 100.0, 2.0, 101.0, 1.0)),
            MarketEvent::Trade(trade(1, 100.0)),
            MarketEvent::Trade(trade(2_000, 101.0)),
            MarketEvent::Trade(trade(4_000, 103.0)),
            MarketEvent::Trade(trade(35_000, 104.0)),
        ];

        let report = run_research(
            events,
            ResearchRunConfig {
                train_size: 1,
                test_size: 1,
                thresholds: vec![0.0],
                min_snapshot_interval_ms: 0,
                min_trade_events: 1,
                min_paper_trades: 1,
                min_walk_forward_splits: 1,
                min_oos_trades: 1,
                min_oos_expectancy_r: 0.0,
                min_positive_oos_split_rate: 0.0,
                min_trend_regimes: 1,
                min_volatility_regimes: 1,
            },
        );

        assert_eq!(report.events_read, 5);
        assert_eq!(report.event_counts.trades, 4);
        assert_eq!(report.price_samples, 4);
        assert!(report.candidates <= report.price_samples);
    }

    #[test]
    fn uses_ticker_as_price_sample_fallback() {
        use crate::events::{Exchange, TickerEvent};

        let report = run_research(
            vec![MarketEvent::Ticker(TickerEvent {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_string(),
                ts_exchange_ms: 1,
                ts_local_ms: 1,
                bid: 99.0,
                ask: 101.0,
                mark: None,
                index: None,
                funding_rate: None,
            })],
            ResearchRunConfig::default(),
        );

        assert_eq!(report.event_counts.tickers, 1);
        assert_eq!(report.price_samples, 1);
    }

    #[test]
    fn uses_kline_as_historical_price_sample_without_trade_count() {
        use crate::events::{Exchange, KlineEvent};

        let report = run_research(
            vec![MarketEvent::Kline(KlineEvent {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_string(),
                interval: "1m".to_string(),
                open_time_ms: 1,
                close_time_ms: 60_000,
                ts_local_ms: 60_000,
                open: 100.0,
                high: 101.0,
                low: 99.0,
                close: 100.5,
                volume: 10.0,
                quote_volume: 1_005.0,
                trades: 3,
            })],
            ResearchRunConfig::default(),
        );

        assert_eq!(report.event_counts.klines, 1);
        assert_eq!(report.event_counts.trades, 0);
        assert_eq!(report.price_samples, 1);
    }

    #[test]
    fn research_labels_use_first_future_sample_after_snapshot() {
        let report = run_research(
            vec![
                MarketEvent::Trade(trade(0, 100.0)),
                MarketEvent::Trade(trade(30_000, 101.0)),
            ],
            ResearchRunConfig {
                train_size: 1,
                test_size: 1,
                thresholds: vec![0.0],
                min_snapshot_interval_ms: 0,
                min_trade_events: 1,
                min_paper_trades: 0,
                min_walk_forward_splits: 0,
                min_oos_trades: 0,
                min_oos_expectancy_r: 0.0,
                min_positive_oos_split_rate: 0.0,
                min_trend_regimes: 0,
                min_volatility_regimes: 0,
            },
        );

        assert!(report.price_samples >= 2);
    }

    #[test]
    fn rolling_features_use_only_past_prices() {
        let past = (0..5)
            .map(|idx| PriceSample {
                ts_local_ms: idx,
                price: 100.0 + idx as f64,
            })
            .collect::<Vec<_>>();

        let features = rolling_features(&past, 110.0);
        assert_eq!(features.value_area_high, Some(104.0));
        assert_eq!(features.value_area_low, Some(100.0));
        assert!(features.delta_z_1s.is_some());
    }

    #[test]
    fn paper_validation_summarizes_equity_curve() {
        let labels = vec![label_with_return(2.0, 1.0), label_with_return(2.0, -0.5)];

        let validation = paper_validate(&labels, 1.0).expect("validation");
        assert_eq!(validation.trades, 2);
        assert_eq!(validation.win_rate, 0.5);
        assert_eq!(validation.total_r, 0.5);
        assert_eq!(validation.max_drawdown_r, 0.5);
    }

    #[test]
    fn walk_forward_validation_trains_threshold_then_tests_next_split() {
        let labels = vec![
            label_with_return(0.0, -1.0),
            label_with_return(2.0, 1.0),
            label_with_return(0.0, -0.5),
            label_with_return(2.0, 0.5),
        ];
        let splits = vec![WalkForwardSplit {
            train_start: 0,
            train_end: 2,
            test_start: 2,
            test_end: 4,
        }];
        let price_samples = vec![PriceSample {
            ts_local_ms: 0,
            price: 100.0,
        }];

        let validation = walk_forward_validate(&labels, &price_samples, &splits, &[0.0, 1.0])
            .expect("walk forward");

        assert_eq!(validation.evaluated_splits, 1);
        assert_eq!(validation.positive_splits, 1);
        assert_eq!(validation.positive_split_rate, 1.0);
        assert_eq!(validation.trades, 1);
        assert_eq!(validation.total_r, 0.5);
        assert_eq!(validation.split_results[0].threshold, 1.0);
        assert_eq!(validation.regime_results.len(), 1);
        assert_eq!(validation.observed_trend_regimes, vec![TrendRegime::Range]);
        assert_eq!(
            validation.observed_volatility_regimes,
            vec![VolatilityRegime::Low]
        );
    }

    #[test]
    fn classifies_split_regime_from_test_prices() {
        let labels = vec![
            label_with_ts_entry_return(0, 100.0, 1.0, 0.0),
            label_with_ts_entry_return(1_000, 100.5, 1.0, 0.0),
            label_with_ts_entry_return(2_000, 101.0, 1.0, 0.0),
        ];
        let price_samples = vec![
            PriceSample {
                ts_local_ms: 0,
                price: 100.0,
            },
            PriceSample {
                ts_local_ms: 1_000,
                price: 100.5,
            },
            PriceSample {
                ts_local_ms: 2_000,
                price: 101.0,
            },
        ];

        let regime = classify_regime(&labels, &price_samples);

        assert_eq!(regime.trend, TrendRegime::Uptrend);
        assert_eq!(regime.volatility, VolatilityRegime::Low);
        assert!(regime.return_pct > 0.0);
    }

    #[test]
    fn scans_regime_windows_from_kline_events() {
        use crate::events::{Exchange, KlineEvent};

        let events = (0..5)
            .map(|idx| {
                MarketEvent::Kline(KlineEvent {
                    exchange: Exchange::Binance,
                    symbol: "BTCUSDT".to_string(),
                    interval: "1m".to_string(),
                    open_time_ms: idx * 60_000,
                    close_time_ms: (idx + 1) * 60_000,
                    ts_local_ms: (idx + 1) * 60_000,
                    open: 100.0 + idx as f64,
                    high: 101.0 + idx as f64,
                    low: 99.0 + idx as f64,
                    close: 100.0 + idx as f64,
                    volume: 1.0,
                    quote_volume: 100.0,
                    trades: 1,
                })
            })
            .collect::<Vec<_>>();

        let report = scan_regimes(events, 3, 1);

        assert_eq!(report.price_samples, 5);
        assert_eq!(report.windows, 3);
        assert!(
            report
                .observed_trend_regimes
                .contains(&TrendRegime::Uptrend)
        );
    }

    #[test]
    fn data_quality_flags_insufficient_research_sample() {
        let quality = data_quality(
            &ResearchEventCounts {
                trades: 10,
                ..Default::default()
            },
            Some(&PaperValidationStats {
                threshold: 1.0,
                trades: 2,
                win_rate: 0.5,
                expectancy_r: 0.1,
                total_r: 0.2,
                max_drawdown_r: 0.1,
            }),
            None,
            0,
            &ResearchRunConfig::default(),
        );

        assert!(!quality.sufficient);
        assert_eq!(quality.issues.len(), 8);
    }

    #[test]
    fn data_quality_requires_positive_oos_evidence() {
        let quality = data_quality(
            &ResearchEventCounts {
                trades: 500,
                ..Default::default()
            },
            Some(&PaperValidationStats {
                threshold: 1.0,
                trades: 100,
                win_rate: 0.5,
                expectancy_r: 0.1,
                total_r: 10.0,
                max_drawdown_r: 1.0,
            }),
            Some(&WalkForwardValidationStats {
                splits: 1,
                evaluated_splits: 1,
                positive_splits: 0,
                positive_split_rate: 0.0,
                trades: 50,
                win_rate: 0.5,
                expectancy_r: -0.01,
                total_r: -0.5,
                max_drawdown_r: 0.5,
                observed_trend_regimes: vec![TrendRegime::Range],
                observed_volatility_regimes: vec![VolatilityRegime::Low],
                regime_results: Vec::new(),
                split_results: Vec::new(),
            }),
            1,
            &ResearchRunConfig::default(),
        );

        assert!(!quality.sufficient);
        assert_eq!(quality.issues.len(), 4);
        assert!(quality.issues[0].starts_with("oos_expectancy_r"));
    }

    #[test]
    fn data_quality_requires_positive_oos_split_rate() {
        let quality = data_quality(
            &ResearchEventCounts {
                trades: 500,
                ..Default::default()
            },
            Some(&PaperValidationStats {
                threshold: 1.0,
                trades: 100,
                win_rate: 0.5,
                expectancy_r: 0.1,
                total_r: 10.0,
                max_drawdown_r: 1.0,
            }),
            Some(&WalkForwardValidationStats {
                splits: 2,
                evaluated_splits: 2,
                positive_splits: 1,
                positive_split_rate: 0.5,
                trades: 50,
                win_rate: 0.5,
                expectancy_r: 0.01,
                total_r: 0.5,
                max_drawdown_r: 0.5,
                observed_trend_regimes: vec![TrendRegime::Range],
                observed_volatility_regimes: vec![VolatilityRegime::Low],
                regime_results: Vec::new(),
                split_results: Vec::new(),
            }),
            2,
            &ResearchRunConfig::default(),
        );

        assert!(!quality.sufficient);
        assert_eq!(quality.issues.len(), 3);
        assert!(quality.issues[0].starts_with("positive_oos_split_rate"));
    }

    #[test]
    fn data_quality_requires_regime_coverage() {
        let quality = data_quality(
            &ResearchEventCounts {
                trades: 500,
                ..Default::default()
            },
            Some(&PaperValidationStats {
                threshold: 1.0,
                trades: 100,
                win_rate: 0.5,
                expectancy_r: 0.1,
                total_r: 10.0,
                max_drawdown_r: 1.0,
            }),
            Some(&WalkForwardValidationStats {
                splits: 2,
                evaluated_splits: 2,
                positive_splits: 2,
                positive_split_rate: 1.0,
                trades: 50,
                win_rate: 0.5,
                expectancy_r: 0.01,
                total_r: 0.5,
                max_drawdown_r: 0.5,
                observed_trend_regimes: vec![TrendRegime::Range],
                observed_volatility_regimes: vec![VolatilityRegime::Low],
                regime_results: Vec::new(),
                split_results: Vec::new(),
            }),
            2,
            &ResearchRunConfig::default(),
        );

        assert!(!quality.sufficient);
        assert_eq!(quality.issues.len(), 2);
        assert!(quality.issues[0].starts_with("trend_regimes"));
        assert!(quality.issues[1].starts_with("volatility_regimes"));
    }

    fn label_with_return(score: f64, return_r_30s: f64) -> CandidateLabel {
        label_with_entry_return(100.0, score, return_r_30s)
    }

    fn label_with_entry_return(entry: f64, score: f64, return_r_30s: f64) -> CandidateLabel {
        label_with_ts_entry_return(0, entry, score, return_r_30s)
    }

    fn label_with_ts_entry_return(
        ts_local_ms: i64,
        entry: f64,
        score: f64,
        return_r_30s: f64,
    ) -> CandidateLabel {
        use crate::research::{CandidateKind, StrategyCandidate};

        CandidateLabel {
            candidate: StrategyCandidate {
                ts_local_ms,
                kind: CandidateKind::LongBreakout,
                entry,
                stop: entry - 1.0,
                target_1r: entry + 1.0,
                target_2r: entry + 2.0,
                score,
            },
            return_r_5s: None,
            return_r_15s: None,
            return_r_30s: Some(return_r_30s),
            hit_1r_before_stop_30s: return_r_30s >= 1.0,
            hit_2r_before_stop_30s: return_r_30s >= 2.0,
            mae_r_30s: return_r_30s.min(0.0),
            mfe_r_30s: return_r_30s.max(0.0),
            time_to_target_ms: None,
            time_to_stop_ms: None,
        }
    }

    fn trade(ts_local_ms: i64, price: f64) -> TradeEvent {
        TradeEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            price,
            qty: 1.0,
            aggressor_side: AggressorSide::Buy,
        }
    }

    fn depth(
        ts_local_ms: i64,
        bid_price: f64,
        bid_qty: f64,
        ask_price: f64,
        ask_qty: f64,
    ) -> DepthDeltaEvent {
        DepthDeltaEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: ts_local_ms,
            ts_local_ms,
            first_update_id: Some(1),
            sequence: Some(1),
            previous_sequence: Some(0),
            bids: vec![DepthLevel {
                price: bid_price,
                qty: bid_qty,
            }],
            asks: vec![DepthLevel {
                price: ask_price,
                qty: ask_qty,
            }],
        }
    }
}
