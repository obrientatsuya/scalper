use serde::Serialize;

const MAX_HORIZON_SAMPLE_LAG_MS: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateKind {
    LongBreakout,
    ShortBreakout,
    MeanReversionLong,
    MeanReversionShort,
    FailedAuctionLong,
    FailedAuctionShort,
}

#[derive(Debug, Clone, Serialize)]
pub struct StrategyCandidate {
    pub ts_local_ms: i64,
    pub kind: CandidateKind,
    pub entry: f64,
    pub stop: f64,
    pub target_1r: f64,
    pub target_2r: f64,
    pub score: f64,
}

#[derive(Debug, Clone, Default)]
pub struct FeatureSnapshot {
    pub ts_local_ms: i64,
    pub price: f64,
    pub vwap: Option<f64>,
    pub vwap_upper_2: Option<f64>,
    pub vwap_lower_2: Option<f64>,
    pub value_area_high: Option<f64>,
    pub value_area_low: Option<f64>,
    pub delta_z_1s: Option<f64>,
    pub obi_20: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CandidateLabel {
    pub candidate: StrategyCandidate,
    pub return_r_5s: Option<f64>,
    pub return_r_15s: Option<f64>,
    pub return_r_30s: Option<f64>,
    pub hit_1r_before_stop_30s: bool,
    pub hit_2r_before_stop_30s: bool,
    pub mae_r_30s: f64,
    pub mfe_r_30s: f64,
    pub time_to_target_ms: Option<i64>,
    pub time_to_stop_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy)]
pub struct PriceSample {
    pub ts_local_ms: i64,
    pub price: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AblationVariant {
    Amt,
    AmtFlow,
    AmtFlowGex,
    AmtFlowGexHeatmap,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct WalkForwardSplit {
    pub train_start: usize,
    pub train_end: usize,
    pub test_start: usize,
    pub test_end: usize,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ThresholdResult {
    pub threshold: f64,
    pub expectancy_r: f64,
    pub samples: usize,
}

pub fn generate_candidates(snapshot: &FeatureSnapshot) -> Vec<StrategyCandidate> {
    let mut candidates = Vec::new();
    let price = snapshot.price;
    let stop_distance = (price * 0.001).max(1.0);
    let delta = snapshot.delta_z_1s.unwrap_or_default();
    let obi = snapshot.obi_20.unwrap_or_default();

    if let Some(vah) = snapshot.value_area_high
        && price > vah
        && delta >= 2.0
    {
        candidates.push(candidate(
            snapshot.ts_local_ms,
            CandidateKind::LongBreakout,
            price,
            price - stop_distance,
            delta.abs() + obi.max(0.0),
        ));
    }

    if let Some(val) = snapshot.value_area_low
        && price < val
        && delta <= -2.0
    {
        candidates.push(candidate(
            snapshot.ts_local_ms,
            CandidateKind::ShortBreakout,
            price,
            price + stop_distance,
            delta.abs() + (-obi).max(0.0),
        ));
    }

    if let Some(upper_2) = snapshot.vwap_upper_2
        && price > upper_2
        && delta < 1.0
    {
        candidates.push(candidate(
            snapshot.ts_local_ms,
            CandidateKind::MeanReversionShort,
            price,
            price + stop_distance,
            1.0 - delta.min(1.0),
        ));
    }

    if let Some(lower_2) = snapshot.vwap_lower_2
        && price < lower_2
        && delta > -1.0
    {
        candidates.push(candidate(
            snapshot.ts_local_ms,
            CandidateKind::MeanReversionLong,
            price,
            price - stop_distance,
            1.0 + delta.max(-1.0),
        ));
    }

    candidates
}

pub fn label_candidate(candidate: StrategyCandidate, future: &[PriceSample]) -> CandidateLabel {
    let risk = (candidate.entry - candidate.stop).abs().max(f64::EPSILON);
    let mut mae_r_30s = 0.0_f64;
    let mut mfe_r_30s = 0.0_f64;
    let mut time_to_target_ms = None;
    let mut time_to_stop_ms = None;
    let mut hit_1r_before_stop_30s = false;
    let mut hit_2r_before_stop_30s = false;

    for sample in future
        .iter()
        .take_while(|sample| sample.ts_local_ms <= candidate.ts_local_ms + 30_000)
    {
        let pnl_r = pnl_r(&candidate, sample.price, risk);
        mfe_r_30s = mfe_r_30s.max(pnl_r);
        mae_r_30s = mae_r_30s.min(pnl_r);

        if time_to_target_ms.is_none() && pnl_r >= 1.0 {
            time_to_target_ms = Some(sample.ts_local_ms - candidate.ts_local_ms);
            hit_1r_before_stop_30s = time_to_stop_ms.is_none();
        }
        if pnl_r >= 2.0 && time_to_stop_ms.is_none() {
            hit_2r_before_stop_30s = true;
        }
        if time_to_stop_ms.is_none() && pnl_r <= -1.0 {
            time_to_stop_ms = Some(sample.ts_local_ms - candidate.ts_local_ms);
        }
    }

    CandidateLabel {
        return_r_5s: return_at(candidate.ts_local_ms + 5_000, &candidate, future, risk),
        return_r_15s: return_at(candidate.ts_local_ms + 15_000, &candidate, future, risk),
        return_r_30s: return_at(candidate.ts_local_ms + 30_000, &candidate, future, risk),
        candidate,
        hit_1r_before_stop_30s,
        hit_2r_before_stop_30s,
        mae_r_30s,
        mfe_r_30s,
        time_to_target_ms,
        time_to_stop_ms,
    }
}

pub fn walk_forward_splits(
    samples: usize,
    train_size: usize,
    test_size: usize,
) -> Vec<WalkForwardSplit> {
    let mut splits = Vec::new();
    let mut start = 0;
    while start + train_size + test_size <= samples {
        splits.push(WalkForwardSplit {
            train_start: start,
            train_end: start + train_size,
            test_start: start + train_size,
            test_end: start + train_size + test_size,
        });
        start += test_size;
    }
    splits
}

pub fn optimize_threshold(
    labels: &[CandidateLabel],
    thresholds: &[f64],
) -> Option<ThresholdResult> {
    thresholds
        .iter()
        .filter_map(|threshold| {
            let returns = labels
                .iter()
                .filter(|label| label.candidate.score >= *threshold)
                .filter_map(|label| label.return_r_30s)
                .collect::<Vec<_>>();
            if returns.is_empty() {
                return None;
            }
            Some(ThresholdResult {
                threshold: *threshold,
                expectancy_r: returns.iter().sum::<f64>() / returns.len() as f64,
                samples: returns.len(),
            })
        })
        .max_by(|left, right| left.expectancy_r.total_cmp(&right.expectancy_r))
}

fn candidate(
    ts_local_ms: i64,
    kind: CandidateKind,
    entry: f64,
    stop: f64,
    score: f64,
) -> StrategyCandidate {
    let risk = (entry - stop).abs();
    let direction = match kind {
        CandidateKind::LongBreakout
        | CandidateKind::MeanReversionLong
        | CandidateKind::FailedAuctionLong => 1.0,
        CandidateKind::ShortBreakout
        | CandidateKind::MeanReversionShort
        | CandidateKind::FailedAuctionShort => -1.0,
    };
    StrategyCandidate {
        ts_local_ms,
        kind,
        entry,
        stop,
        target_1r: entry + direction * risk,
        target_2r: entry + direction * risk * 2.0,
        score,
    }
}

fn return_at(
    target_ts_ms: i64,
    candidate: &StrategyCandidate,
    future: &[PriceSample],
    risk: f64,
) -> Option<f64> {
    future
        .iter()
        .find(|sample| {
            sample.ts_local_ms >= target_ts_ms
                && sample.ts_local_ms <= target_ts_ms + MAX_HORIZON_SAMPLE_LAG_MS
        })
        .map(|sample| pnl_r(candidate, sample.price, risk))
}

fn pnl_r(candidate: &StrategyCandidate, price: f64, risk: f64) -> f64 {
    match candidate.kind {
        CandidateKind::LongBreakout
        | CandidateKind::MeanReversionLong
        | CandidateKind::FailedAuctionLong => (price - candidate.entry) / risk,
        CandidateKind::ShortBreakout
        | CandidateKind::MeanReversionShort
        | CandidateKind::FailedAuctionShort => (candidate.entry - price) / risk,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_long_breakout_candidate() {
        let candidates = generate_candidates(&FeatureSnapshot {
            ts_local_ms: 1,
            price: 101.0,
            value_area_high: Some(100.0),
            delta_z_1s: Some(2.5),
            obi_20: Some(0.2),
            ..Default::default()
        });

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].kind, CandidateKind::LongBreakout);
        assert_eq!(candidates[0].target_1r, 102.0);
    }

    #[test]
    fn labels_30s_returns_mae_mfe_and_hits() {
        let candidate = candidate(0, CandidateKind::LongBreakout, 100.0, 99.0, 1.0);
        let label = label_candidate(
            candidate,
            &[
                PriceSample {
                    ts_local_ms: 5_000,
                    price: 100.5,
                },
                PriceSample {
                    ts_local_ms: 15_000,
                    price: 102.0,
                },
                PriceSample {
                    ts_local_ms: 30_000,
                    price: 101.0,
                },
            ],
        );

        assert_eq!(label.return_r_5s, Some(0.5));
        assert_eq!(label.return_r_15s, Some(2.0));
        assert_eq!(label.return_r_30s, Some(1.0));
        assert!(label.hit_1r_before_stop_30s);
        assert!(label.hit_2r_before_stop_30s);
        assert_eq!(label.mfe_r_30s, 2.0);
    }

    #[test]
    fn labels_do_not_cross_large_data_gaps() {
        let candidate = candidate(0, CandidateKind::LongBreakout, 100.0, 99.0, 1.0);
        let label = label_candidate(
            candidate,
            &[PriceSample {
                ts_local_ms: 45_000,
                price: 105.0,
            }],
        );

        assert_eq!(label.return_r_30s, None);
        assert!(!label.hit_1r_before_stop_30s);
        assert_eq!(label.mfe_r_30s, 0.0);
    }

    #[test]
    fn builds_walk_forward_splits() {
        let splits = walk_forward_splits(100, 60, 20);

        assert_eq!(splits.len(), 2);
        assert_eq!(splits[0].train_start, 0);
        assert_eq!(splits[1].test_start, 80);
    }

    #[test]
    fn optimizes_threshold_by_expectancy() {
        let weak = StrategyCandidate {
            score: 0.5,
            ..candidate(0, CandidateKind::LongBreakout, 100.0, 99.0, 0.5)
        };
        let strong = StrategyCandidate {
            score: 2.0,
            ..candidate(0, CandidateKind::LongBreakout, 100.0, 99.0, 2.0)
        };
        let labels = vec![
            CandidateLabel {
                candidate: weak,
                return_r_30s: Some(-1.0),
                return_r_5s: None,
                return_r_15s: None,
                hit_1r_before_stop_30s: false,
                hit_2r_before_stop_30s: false,
                mae_r_30s: -1.0,
                mfe_r_30s: 0.0,
                time_to_target_ms: None,
                time_to_stop_ms: Some(1),
            },
            CandidateLabel {
                candidate: strong,
                return_r_30s: Some(2.0),
                return_r_5s: None,
                return_r_15s: None,
                hit_1r_before_stop_30s: true,
                hit_2r_before_stop_30s: true,
                mae_r_30s: 0.0,
                mfe_r_30s: 2.0,
                time_to_target_ms: Some(1),
                time_to_stop_ms: None,
            },
        ];

        let result = optimize_threshold(&labels, &[0.0, 1.0]).expect("threshold");
        assert_eq!(result.threshold, 1.0);
        assert_eq!(result.expectancy_r, 2.0);
    }
}
