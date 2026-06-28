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
pub struct OrderBookSnapshotEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub ts_local_ms: i64,
    pub last_update_id: u64,
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
pub struct KlineEvent {
    pub exchange: Exchange,
    pub symbol: String,
    pub interval: String,
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    pub ts_local_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
    pub quote_volume: f64,
    pub trades: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptionType {
    Call,
    Put,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionGreekEvent {
    pub exchange: Exchange,
    pub underlying: String,
    pub option_symbol: String,
    pub expiry_ms: i64,
    pub strike: f64,
    pub option_type: OptionType,
    pub ts_local_ms: i64,
    pub mark_price: f64,
    pub open_interest_contracts: f64,
    pub contract_unit: f64,
    pub gamma: f64,
    pub delta: Option<f64>,
    pub iv: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MarketEvent {
    Trade(TradeEvent),
    DepthDelta(DepthDeltaEvent),
    OrderBookSnapshot(OrderBookSnapshotEvent),
    Ticker(TickerEvent),
    Kline(KlineEvent),
    OptionGreek(OptionGreekEvent),
    Heartbeat { component: String, ts_local_ms: i64 },
}
