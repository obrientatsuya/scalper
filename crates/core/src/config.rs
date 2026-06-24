use crate::events::Exchange;
use serde::{Deserialize, Serialize};
use std::{path::Path, time::Duration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub app: AppSection,
    pub health: HealthSection,
    pub channels: ChannelSection,
    pub heartbeat: HeartbeatSection,
    #[serde(default)]
    pub replay: Option<ReplaySection>,
    pub storage: StorageSection,
    pub venues: Vec<VenueConfig>,
    pub risk: RiskConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppSection {
    pub mode: RunMode,
    pub symbol: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Capture,
    Replay,
    PaperLive,
    FuturesDemo,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthSection {
    pub bind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelSection {
    pub market_events_capacity: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatSection {
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub stale_after: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplaySection {
    pub input_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    pub enabled: bool,
    pub root_dir: String,
    pub flush_batch_size: usize,
    #[serde(with = "humantime_serde")]
    pub flush_interval: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    pub exchange: Exchange,
    pub enabled: bool,
    pub market_data: bool,
    pub execution: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub starting_capital_brl: f64,
    pub risk_per_trade_brl: f64,
    pub max_daily_loss_brl: f64,
    pub max_weekly_loss_brl: f64,
    pub max_concurrent_positions: u32,
    pub leverage_live_hard_cap_initial: f64,
    pub leverage_paper_stress_cap: f64,
    pub max_fee_to_target_ratio: f64,
    pub max_slippage_to_stop_ratio: f64,
    pub max_margin_fraction_per_trade: f64,
}

impl AppConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let cfg = config::Config::builder()
            .add_source(config::File::from(path.as_ref()))
            .add_source(config::Environment::with_prefix("SCALPER").separator("__"))
            .build()?;
        Ok(cfg.try_deserialize()?)
    }
}
