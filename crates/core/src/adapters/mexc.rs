use crate::events::{
    AggressorSide, DepthDeltaEvent, DepthLevel, Exchange, MarketEvent, TickerEvent, TradeEvent,
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const MEXC_FUTURES_WS: &str = "wss://contract.mexc.com/edge";

pub fn spawn_mexc_market_data(
    symbol: String,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);
        let symbol = mexc_symbol(&symbol);

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            info!(symbol = %symbol, "connecting mexc market data");
            let result = run_mexc_stream(&symbol, market_tx.clone(), shutdown.clone()).await;

            if shutdown.is_cancelled() {
                return;
            }

            match result {
                Ok(()) => {
                    warn!("mexc stream ended");
                    backoff = Duration::from_secs(1);
                }
                Err(error) => {
                    warn!(%error, ?backoff, "mexc stream failed");
                }
            }

            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = sleep(backoff) => {}
            }

            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    })
}

async fn run_mexc_stream(
    symbol: &str,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(MEXC_FUTURES_WS).await?;
    let (mut write, mut read) = ws.split();
    info!(symbol, "mexc market data connected");

    for subscription in subscriptions(symbol) {
        write.send(Message::Text(subscription.into())).await?;
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };

                match message? {
                    Message::Text(text) => {
                        for event in parse_mexc_message(&text)? {
                            if market_tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Binary(bytes) => {
                        let text = String::from_utf8(bytes.to_vec())?;
                        for event in parse_mexc_message(&text)? {
                            if market_tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Ping(bytes) => {
                        write.send(Message::Pong(bytes)).await?;
                    }
                    Message::Pong(_) => {}
                    Message::Close(frame) => {
                        warn!(?frame, "mexc stream closed");
                        return Ok(());
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

fn subscriptions(symbol: &str) -> Vec<String> {
    ["sub.deal", "sub.ticker", "sub.depth"]
        .into_iter()
        .map(|method| {
            json!({
                "method": method,
                "param": { "symbol": symbol },
                "gzip": false
            })
            .to_string()
        })
        .collect()
}

pub fn parse_mexc_message(text: &str) -> anyhow::Result<Vec<MarketEvent>> {
    let message: MexcMessage = serde_json::from_str(text)?;
    match message.channel.as_deref() {
        Some("push.deal") => Ok(vec![MarketEvent::Trade(parse_deal(message)?)]),
        Some("push.ticker") => Ok(vec![MarketEvent::Ticker(parse_ticker(message)?)]),
        Some("push.depth") => Ok(vec![MarketEvent::DepthDelta(parse_depth(message)?)]),
        _ => Ok(Vec::new()),
    }
}

fn parse_deal(message: MexcMessage) -> anyhow::Result<TradeEvent> {
    let deal: MexcDeal = serde_json::from_value(message.data)?;
    Ok(TradeEvent {
        exchange: Exchange::Mexc,
        symbol: normalize_symbol(message.symbol.as_deref().unwrap_or_default()),
        ts_exchange_ms: deal.trade_time_ms,
        ts_local_ms: now_ms(),
        price: deal.price,
        qty: deal.volume,
        aggressor_side: match deal.trade_direction {
            1 => AggressorSide::Buy,
            2 => AggressorSide::Sell,
            _ => AggressorSide::Unknown,
        },
    })
}

fn parse_ticker(message: MexcMessage) -> anyhow::Result<TickerEvent> {
    let ticker: MexcTicker = serde_json::from_value(message.data)?;
    Ok(TickerEvent {
        exchange: Exchange::Mexc,
        symbol: normalize_symbol(&ticker.symbol),
        ts_exchange_ms: ticker.timestamp.or(message.ts).unwrap_or_else(now_ms),
        ts_local_ms: now_ms(),
        bid: ticker.bid1.unwrap_or(ticker.last_price),
        ask: ticker.ask1.unwrap_or(ticker.last_price),
        mark: Some(ticker.fair_price.unwrap_or(ticker.last_price)),
        index: ticker.index_price,
        funding_rate: ticker.funding_rate,
    })
}

fn parse_depth(message: MexcMessage) -> anyhow::Result<DepthDeltaEvent> {
    let depth: MexcDepth = serde_json::from_value(message.data)?;
    Ok(DepthDeltaEvent {
        exchange: Exchange::Mexc,
        symbol: normalize_symbol(message.symbol.as_deref().unwrap_or_default()),
        ts_exchange_ms: message.ts.unwrap_or_else(now_ms),
        ts_local_ms: now_ms(),
        first_update_id: Some(depth.version),
        sequence: Some(depth.version),
        previous_sequence: None,
        bids: parse_levels(depth.bids),
        asks: parse_levels(depth.asks),
    })
}

fn parse_levels(levels: Vec<[f64; 3]>) -> Vec<DepthLevel> {
    levels
        .into_iter()
        .map(|[price, _orders, qty]| DepthLevel { price, qty })
        .collect()
}

fn mexc_symbol(symbol: &str) -> String {
    if symbol.contains('_') {
        return symbol.to_ascii_uppercase();
    }
    let upper = symbol.to_ascii_uppercase();
    upper
        .strip_suffix("USDT")
        .map(|base| format!("{base}_USDT"))
        .unwrap_or(upper)
}

fn normalize_symbol(symbol: &str) -> String {
    symbol.replace('_', "")
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Deserialize)]
struct MexcMessage {
    channel: Option<String>,
    data: Value,
    symbol: Option<String>,
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MexcDeal {
    #[serde(rename = "p")]
    price: f64,
    #[serde(rename = "v")]
    volume: f64,
    #[serde(rename = "T")]
    trade_direction: i64,
    #[serde(rename = "t")]
    trade_time_ms: i64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MexcTicker {
    symbol: String,
    last_price: f64,
    bid1: Option<f64>,
    ask1: Option<f64>,
    fair_price: Option<f64>,
    index_price: Option<f64>,
    funding_rate: Option<f64>,
    timestamp: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MexcDepth {
    bids: Vec<[f64; 3]>,
    asks: Vec<[f64; 3]>,
    version: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_symbol_to_mexc_contract_symbol() {
        assert_eq!(mexc_symbol("BTCUSDT"), "BTC_USDT");
        assert_eq!(mexc_symbol("BTC_USDT"), "BTC_USDT");
    }

    #[test]
    fn parses_deal() {
        let text = r#"{
          "channel":"push.deal",
          "data":{"M":1,"O":1,"T":2,"p":6866.5,"t":1587442049632,"v":2096},
          "symbol":"BTC_USDT",
          "ts":1587442022003
        }"#;

        let events = parse_mexc_message(text).expect("parse");
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };

        assert_eq!(trade.exchange, Exchange::Mexc);
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.aggressor_side, AggressorSide::Sell);
        assert_eq!(trade.price, 6866.5);
        assert_eq!(trade.qty, 2096.0);
    }

    #[test]
    fn parses_ticker() {
        let text = r#"{
          "channel":"push.ticker",
          "data":{"ask1":6866.5,"bid1":6865,"fairPrice":6867.4,"fundingRate":0.0008,"indexPrice":6861.6,"lastPrice":6865.5,"symbol":"BTC_USDT","timestamp":1587442022003},
          "symbol":"BTC_USDT",
          "ts":1587442022003
        }"#;

        let events = parse_mexc_message(text).expect("parse");
        let MarketEvent::Ticker(ticker) = &events[0] else {
            panic!("expected ticker");
        };

        assert_eq!(ticker.exchange, Exchange::Mexc);
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert_eq!(ticker.bid, 6865.0);
        assert_eq!(ticker.ask, 6866.5);
        assert_eq!(ticker.mark, Some(6867.4));
        assert_eq!(ticker.funding_rate, Some(0.0008));
    }

    #[test]
    fn parses_depth() {
        let text = r#"{
          "channel":"push.depth",
          "data":{"asks":[[6859.5,3251,1]],"bids":[[6858.5,100,2]],"version":96801927},
          "symbol":"BTC_USDT",
          "ts":1587442022003
        }"#;

        let events = parse_mexc_message(text).expect("parse");
        let MarketEvent::DepthDelta(depth) = &events[0] else {
            panic!("expected depth");
        };

        assert_eq!(depth.exchange, Exchange::Mexc);
        assert_eq!(depth.sequence, Some(96801927));
        assert_eq!(depth.bids[0].price, 6858.5);
        assert_eq!(depth.bids[0].qty, 2.0);
        assert_eq!(depth.asks[0].qty, 1.0);
    }
}
