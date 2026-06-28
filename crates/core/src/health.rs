use crate::{
    execution::ExecutionStats, gex::GexStats, heatmap::HeatmapStats, hidden::HiddenLiquidityStats,
    latency::LatencyStats, orderbook::OrderBookStats, orderflow::OrderflowStats,
    paper::PaperBrokerStats, profile::ProfileStats, state::RuntimeState, storage::StorageStats,
};
use serde::Serialize;
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{RwLock, watch};

#[derive(Debug, Clone)]
pub struct HealthHandle {
    state_tx: watch::Sender<RuntimeState>,
    state_rx: watch::Receiver<RuntimeState>,
    started_at: Instant,
    market_events_seen: Arc<AtomicU64>,
    execution_stats: Arc<RwLock<Option<ExecutionStats>>>,
    gex_stats: Arc<RwLock<Option<GexStats>>>,
    hidden_stats: Arc<RwLock<Option<HiddenLiquidityStats>>>,
    latency_stats: Arc<RwLock<Option<LatencyStats>>>,
    heatmap_stats: Arc<RwLock<Option<HeatmapStats>>>,
    orderflow_stats: Arc<RwLock<Option<OrderflowStats>>>,
    paper_stats: Arc<RwLock<Option<PaperBrokerStats>>>,
    profile_stats: Arc<RwLock<Option<ProfileStats>>>,
    orderbook_stats: Arc<RwLock<Option<OrderBookStats>>>,
    storage_stats: Arc<RwLock<Option<StorageStats>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub state: RuntimeState,
    pub allows_signals: bool,
    pub uptime_seconds: u64,
    pub market_events_seen: u64,
    pub execution: Option<ExecutionStats>,
    pub gex: Option<GexStats>,
    pub hidden_liquidity: Option<HiddenLiquidityStats>,
    pub latency: Option<LatencyStats>,
    pub heatmap: Option<HeatmapStats>,
    pub orderflow: Option<OrderflowStats>,
    pub paper: Option<PaperBrokerStats>,
    pub profile: Option<ProfileStats>,
    pub orderbook: Option<OrderBookStats>,
    pub storage: Option<StorageStats>,
}

impl HealthHandle {
    pub fn new(initial: RuntimeState) -> Self {
        let (state_tx, state_rx) = watch::channel(initial);
        Self {
            state_tx,
            state_rx,
            started_at: Instant::now(),
            market_events_seen: Arc::new(AtomicU64::new(0)),
            execution_stats: Arc::new(RwLock::new(None)),
            gex_stats: Arc::new(RwLock::new(None)),
            hidden_stats: Arc::new(RwLock::new(None)),
            latency_stats: Arc::new(RwLock::new(None)),
            heatmap_stats: Arc::new(RwLock::new(None)),
            orderflow_stats: Arc::new(RwLock::new(None)),
            paper_stats: Arc::new(RwLock::new(None)),
            profile_stats: Arc::new(RwLock::new(None)),
            orderbook_stats: Arc::new(RwLock::new(None)),
            storage_stats: Arc::new(RwLock::new(None)),
        }
    }

    pub fn set_state(&self, state: RuntimeState) {
        let _ = self.state_tx.send(state);
    }

    pub fn subscribe_state(&self) -> watch::Receiver<RuntimeState> {
        self.state_rx.clone()
    }

    pub fn inc_market_events(&self) {
        self.market_events_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub async fn set_orderbook_stats(&self, stats: OrderBookStats) {
        *self.orderbook_stats.write().await = Some(stats);
    }

    pub async fn set_execution_stats(&self, stats: ExecutionStats) {
        *self.execution_stats.write().await = Some(stats);
    }

    pub async fn set_gex_stats(&self, stats: GexStats) {
        *self.gex_stats.write().await = Some(stats);
    }

    pub async fn set_hidden_stats(&self, stats: HiddenLiquidityStats) {
        *self.hidden_stats.write().await = Some(stats);
    }

    pub async fn set_latency_stats(&self, stats: LatencyStats) {
        *self.latency_stats.write().await = Some(stats);
    }

    pub async fn set_heatmap_stats(&self, stats: HeatmapStats) {
        *self.heatmap_stats.write().await = Some(stats);
    }

    pub async fn set_orderflow_stats(&self, stats: OrderflowStats) {
        *self.orderflow_stats.write().await = Some(stats);
    }

    pub async fn set_paper_stats(&self, stats: PaperBrokerStats) {
        *self.paper_stats.write().await = Some(stats);
    }

    pub async fn set_profile_stats(&self, stats: ProfileStats) {
        *self.profile_stats.write().await = Some(stats);
    }

    pub async fn set_storage_stats(&self, stats: StorageStats) {
        *self.storage_stats.write().await = Some(stats);
    }

    pub async fn snapshot(&self) -> HealthSnapshot {
        let state = *self.state_rx.borrow();
        HealthSnapshot {
            state,
            allows_signals: state.allows_signals(),
            uptime_seconds: self.started_at.elapsed().as_secs(),
            market_events_seen: self.market_events_seen.load(Ordering::Relaxed),
            execution: self.execution_stats.read().await.clone(),
            gex: self.gex_stats.read().await.clone(),
            hidden_liquidity: self.hidden_stats.read().await.clone(),
            latency: self.latency_stats.read().await.clone(),
            heatmap: self.heatmap_stats.read().await.clone(),
            orderflow: self.orderflow_stats.read().await.clone(),
            paper: self.paper_stats.read().await.clone(),
            profile: self.profile_stats.read().await.clone(),
            orderbook: self.orderbook_stats.read().await.clone(),
            storage: self.storage_stats.read().await.clone(),
        }
    }
}
