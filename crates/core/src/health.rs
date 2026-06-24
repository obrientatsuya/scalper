use crate::{orderbook::OrderBookStats, state::RuntimeState, storage::StorageStats};
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
    orderbook_stats: Arc<RwLock<Option<OrderBookStats>>>,
    storage_stats: Arc<RwLock<Option<StorageStats>>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthSnapshot {
    pub state: RuntimeState,
    pub allows_signals: bool,
    pub uptime_seconds: u64,
    pub market_events_seen: u64,
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
            orderbook: self.orderbook_stats.read().await.clone(),
            storage: self.storage_stats.read().await.clone(),
        }
    }
}
