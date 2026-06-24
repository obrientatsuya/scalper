use crate::{config::AppConfig, events::MarketEvent, health::HealthHandle, state::RuntimeState};
use tokio::{
    sync::mpsc,
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug)]
pub struct Supervisor {
    config: AppConfig,
    health: HealthHandle,
    market_rx: mpsc::Receiver<MarketEvent>,
    shutdown: CancellationToken,
}

impl Supervisor {
    pub fn new(
        config: AppConfig,
        health: HealthHandle,
        market_rx: mpsc::Receiver<MarketEvent>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            config,
            health,
            market_rx,
            shutdown,
        }
    }

    pub async fn run(mut self) -> anyhow::Result<()> {
        info!(mode = ?self.config.app.mode, symbol = %self.config.app.symbol, "supervisor starting");
        self.health.set_state(RuntimeState::SyncingMarketData);

        let mut heartbeat = interval(self.config.heartbeat.interval);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let mut saw_market_event = false;

        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    self.health.set_state(RuntimeState::Shutdown);
                    info!("supervisor shutdown");
                    return Ok(());
                }
                _ = heartbeat.tick() => {
                    if saw_market_event {
                        self.health.set_state(RuntimeState::PaperReady);
                    } else {
                        self.health.set_state(RuntimeState::WarmingFeatures);
                    }
                }
                event = self.market_rx.recv() => {
                    match event {
                        Some(event) => {
                            saw_market_event = true;
                            self.health.inc_market_events();
                            if matches!(event, MarketEvent::Heartbeat { .. }) {
                                info!(?event, "market heartbeat");
                            }
                        }
                        None => {
                            self.health.set_state(RuntimeState::Degraded);
                            warn!("market event channel closed");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}
