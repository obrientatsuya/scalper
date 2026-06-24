use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    Binance,
    Mexc,
    Deribit,
    Okx,
    Bybit,
    Hyperliquid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggressorSide {
    Buy,
    Sell,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub ts_exchange_ms: i64,
    pub ts_local_ms: i64,
    pub price: f64,
    pub qty: f64,
    pub aggressor_side: AggressorSide,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthLevel {
    pub price: f64,
    pub qty: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthDeltaEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub ts_exchange_ms: i64,
    pub ts_local_ms: i64,
    pub first_update_id: Option<u64>,
    pub sequence: Option<u64>,
    pub previous_sequence: Option<u64>,
    pub bids: Vec<DepthLevel>,
    pub asks: Vec<DepthLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TickerEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub ts_exchange_ms: i64,
    pub ts_local_ms: i64,
    pub bid: f64,
    pub ask: f64,
    pub mark: Option<f64>,
    pub index: Option<f64>,
    pub funding_rate: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketEvent {
    Trade(TradeEvent),
    DepthDelta(DepthDeltaEvent),
    Ticker(TickerEvent),
    Heartbeat { component: String, ts_local_ms: i64 },
}
