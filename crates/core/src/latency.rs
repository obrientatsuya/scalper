use crate::events::{
    DepthDeltaEvent, KlineEvent, MarketEvent, OptionGreekEvent, OrderBookSnapshotEvent,
    TickerEvent, TradeEvent,
};
use serde::Serialize;

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencyStats {
    pub events_seen: u64,
    pub events_with_exchange_ts: u64,
    pub event_lag_ms_last: Option<i64>,
    pub event_lag_ms_min: Option<i64>,
    pub event_lag_ms_max: Option<i64>,
    pub event_lag_ms_avg: Option<f64>,
    pub ws_jitter_ms_last: Option<u64>,
    pub ws_jitter_ms_min: Option<u64>,
    pub ws_jitter_ms_max: Option<u64>,
    pub ws_jitter_ms_avg: Option<f64>,
    pub queue_depths: QueueDepthStats,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueDepthStats {
    pub raw_market_rx: usize,
    pub supervisor_tx: usize,
    pub orderflow_tx: usize,
    pub profile_tx: usize,
    pub gex_tx: usize,
    pub hidden_tx: usize,
    pub heatmap_tx: usize,
    pub orderbook_tx: usize,
    pub storage_tx: usize,
}

#[derive(Debug, Clone, Default)]
pub struct LatencyTracker {
    stats: LatencyStats,
    lag_sum_ms: i128,
    jitter_sum_ms: u128,
    last_local_ts_ms: Option<i64>,
    last_inter_arrival_ms: Option<u64>,
}

impl LatencyTracker {
    pub fn observe(&mut self, event: &MarketEvent, queue_depths: QueueDepthStats) -> LatencyStats {
        self.stats.events_seen += 1;
        self.stats.queue_depths = queue_depths;

        if let Some(lag_ms) = event_lag_ms(event) {
            self.stats.events_with_exchange_ts += 1;
            self.lag_sum_ms += lag_ms as i128;
            self.stats.event_lag_ms_last = Some(lag_ms);
            self.stats.event_lag_ms_min = Some(match self.stats.event_lag_ms_min {
                Some(current) => current.min(lag_ms),
                None => lag_ms,
            });
            self.stats.event_lag_ms_max = Some(match self.stats.event_lag_ms_max {
                Some(current) => current.max(lag_ms),
                None => lag_ms,
            });
            self.stats.event_lag_ms_avg =
                Some(self.lag_sum_ms as f64 / self.stats.events_with_exchange_ts as f64);
        }

        let local_ts_ms = event_ts_local_ms(event);
        if let Some(previous_local_ts_ms) = self.last_local_ts_ms {
            let inter_arrival_ms = local_ts_ms.abs_diff(previous_local_ts_ms);
            if let Some(previous_inter_arrival_ms) = self.last_inter_arrival_ms {
                let jitter_ms = inter_arrival_ms.abs_diff(previous_inter_arrival_ms);
                self.jitter_sum_ms += jitter_ms as u128;
                let samples = self.stats.events_seen.saturating_sub(2);

                self.stats.ws_jitter_ms_last = Some(jitter_ms);
                self.stats.ws_jitter_ms_min = Some(match self.stats.ws_jitter_ms_min {
                    Some(current) => current.min(jitter_ms),
                    None => jitter_ms,
                });
                self.stats.ws_jitter_ms_max = Some(match self.stats.ws_jitter_ms_max {
                    Some(current) => current.max(jitter_ms),
                    None => jitter_ms,
                });
                if samples > 0 {
                    self.stats.ws_jitter_ms_avg = Some(self.jitter_sum_ms as f64 / samples as f64);
                }
            }
            self.last_inter_arrival_ms = Some(inter_arrival_ms);
        }
        self.last_local_ts_ms = Some(local_ts_ms);

        self.stats.clone()
    }
}

pub fn event_ts_local_ms(event: &MarketEvent) -> i64 {
    match event {
        MarketEvent::Trade(TradeEvent { ts_local_ms, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { ts_local_ms, .. })
        | MarketEvent::OrderBookSnapshot(OrderBookSnapshotEvent { ts_local_ms, .. })
        | MarketEvent::Ticker(TickerEvent { ts_local_ms, .. })
        | MarketEvent::Kline(KlineEvent { ts_local_ms, .. })
        | MarketEvent::OptionGreek(OptionGreekEvent { ts_local_ms, .. })
        | MarketEvent::Heartbeat { ts_local_ms, .. } => *ts_local_ms,
    }
}

fn event_ts_exchange_ms(event: &MarketEvent) -> Option<i64> {
    match event {
        MarketEvent::Trade(TradeEvent { ts_exchange_ms, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { ts_exchange_ms, .. })
        | MarketEvent::Ticker(TickerEvent { ts_exchange_ms, .. }) => Some(*ts_exchange_ms),
        MarketEvent::Kline(KlineEvent { close_time_ms, .. }) => Some(*close_time_ms),
        MarketEvent::OptionGreek(_)
        | MarketEvent::OrderBookSnapshot(_)
        | MarketEvent::Heartbeat { .. } => None,
    }
}

fn event_lag_ms(event: &MarketEvent) -> Option<i64> {
    Some(event_ts_local_ms(event) - event_ts_exchange_ms(event)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{AggressorSide, Exchange};

    #[test]
    fn tracks_event_lag() {
        let mut tracker = LatencyTracker::default();

        let stats = tracker.observe(
            &MarketEvent::Trade(TradeEvent {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_string(),
                ts_exchange_ms: 100,
                ts_local_ms: 125,
                price: 1.0,
                qty: 1.0,
                aggressor_side: AggressorSide::Buy,
            }),
            QueueDepthStats::default(),
        );

        assert_eq!(stats.events_seen, 1);
        assert_eq!(stats.events_with_exchange_ts, 1);
        assert_eq!(stats.event_lag_ms_last, Some(25));
        assert_eq!(stats.event_lag_ms_min, Some(25));
        assert_eq!(stats.event_lag_ms_max, Some(25));
        assert_eq!(stats.event_lag_ms_avg, Some(25.0));
    }

    #[test]
    fn tracks_ws_jitter_from_local_intervals() {
        let mut tracker = LatencyTracker::default();

        for ts_local_ms in [100, 120, 170] {
            tracker.observe(
                &MarketEvent::Heartbeat {
                    component: "test".to_string(),
                    ts_local_ms,
                },
                QueueDepthStats::default(),
            );
        }

        let stats = tracker.observe(
            &MarketEvent::Heartbeat {
                component: "test".to_string(),
                ts_local_ms: 210,
            },
            QueueDepthStats::default(),
        );

        assert_eq!(stats.ws_jitter_ms_last, Some(10));
        assert_eq!(stats.ws_jitter_ms_min, Some(10));
        assert_eq!(stats.ws_jitter_ms_max, Some(30));
        assert_eq!(stats.ws_jitter_ms_avg, Some(20.0));
    }

    #[test]
    fn stores_queue_depths() {
        let mut tracker = LatencyTracker::default();
        let stats = tracker.observe(
            &MarketEvent::Heartbeat {
                component: "test".to_string(),
                ts_local_ms: 1,
            },
            QueueDepthStats {
                raw_market_rx: 1,
                supervisor_tx: 2,
                orderflow_tx: 3,
                profile_tx: 4,
                gex_tx: 5,
                hidden_tx: 6,
                heatmap_tx: 7,
                orderbook_tx: 8,
                storage_tx: 9,
            },
        );

        assert_eq!(stats.queue_depths.raw_market_rx, 1);
        assert_eq!(stats.queue_depths.supervisor_tx, 2);
        assert_eq!(stats.queue_depths.orderflow_tx, 3);
        assert_eq!(stats.queue_depths.profile_tx, 4);
        assert_eq!(stats.queue_depths.gex_tx, 5);
        assert_eq!(stats.queue_depths.hidden_tx, 6);
        assert_eq!(stats.queue_depths.heatmap_tx, 7);
        assert_eq!(stats.queue_depths.orderbook_tx, 8);
        assert_eq!(stats.queue_depths.storage_tx, 9);
    }
}
