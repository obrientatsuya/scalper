use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RuntimeState {
    Booting,
    SyncingMarketData,
    WarmingFeatures,
    PaperReady,
    TradingReady,
    Degraded,
    KillSwitch,
    Shutdown,
}

impl RuntimeState {
    pub fn allows_signals(self) -> bool {
        matches!(self, Self::PaperReady | Self::TradingReady)
    }
}

impl fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Booting => "BOOTING",
            Self::SyncingMarketData => "SYNCING_MARKET_DATA",
            Self::WarmingFeatures => "WARMING_FEATURES",
            Self::PaperReady => "PAPER_READY",
            Self::TradingReady => "TRADING_READY",
            Self::Degraded => "DEGRADED",
            Self::KillSwitch => "KILL_SWITCH",
            Self::Shutdown => "SHUTDOWN",
        };
        f.write_str(value)
    }
}
