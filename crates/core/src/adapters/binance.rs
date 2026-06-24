use crate::events::{
    AggressorSide, DepthDeltaEvent, DepthLevel, Exchange, MarketEvent, TickerEvent, TradeEvent,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::Value;
use std::time::Duration;
use tokio::{sync::mpsc, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const BINANCE_USD_M_WS_BASE: &str = "wss://fstream.binance.com/stream?streams=";

pub fn spawn_binance_market_data(
    symbol: String,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut backoff = Duration::from_secs(1);

        loop {
            if shutdown.is_cancelled() {
                return;
            }

            let url = combined_stream_url(&symbol);
            info!(%url, "connecting binance market data");

            let result = run_binance_stream(&url, market_tx.clone(), shutdown.clone()).await;

            if shutdown.is_cancelled() {
                return;
            }

            match result {
                Ok(()) => {
                    warn!("binance stream ended");
                    backoff = Duration::from_secs(1);
                }
                Err(error) => {
                    warn!(%error, ?backoff, "binance stream failed");
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

fn combined_stream_url(symbol: &str) -> String {
    let symbol = symbol.to_ascii_lowercase();
    let streams = [
        format!("{symbol}@aggTrade"),
        format!("{symbol}@bookTicker"),
        format!("{symbol}@depth@100ms"),
    ]
    .join("/");

    format!("{BINANCE_USD_M_WS_BASE}{streams}")
}

async fn run_binance_stream(
    url: &str,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let (ws, _) = connect_async(url).await?;
    let (_write, mut read) = ws.split();
    info!("binance market data connected");

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            message = read.next() => {
                let Some(message) = message else {
                    return Ok(());
                };

                match message? {
                    Message::Text(text) => {
                        for event in parse_combined_message(&text)? {
                            if market_tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Binary(bytes) => {
                        let text = String::from_utf8(bytes.to_vec())?;
                        for event in parse_combined_message(&text)? {
                            if market_tx.send(event).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    Message::Ping(_) | Message::Pong(_) => {}
                    Message::Close(frame) => {
                        warn!(?frame, "binance stream closed");
                        return Ok(());
                    }
                    Message::Frame(_) => {}
                }
            }
        }
    }
}

pub fn parse_combined_message(text: &str) -> anyhow::Result<Vec<MarketEvent>> {
    let message: CombinedMessage = serde_json::from_str(text)?;
    parse_stream_data(&message.stream, message.data)
}

fn parse_stream_data(stream: &str, data: Value) -> anyhow::Result<Vec<MarketEvent>> {
    if stream.ends_with("@aggTrade") {
        return Ok(vec![MarketEvent::Trade(parse_agg_trade(data)?)]);
    }

    if stream.ends_with("@bookTicker") {
        return Ok(vec![MarketEvent::Ticker(parse_book_ticker(data)?)]);
    }

    if stream.contains("@depth") {
        return Ok(vec![MarketEvent::DepthDelta(parse_depth_delta(data)?)]);
    }

    Ok(Vec::new())
}

fn parse_agg_trade(data: Value) -> anyhow::Result<TradeEvent> {
    let trade: BinanceAggTrade = serde_json::from_value(data)?;
    Ok(TradeEvent {
        exchange: Exchange::Binance,
        symbol: trade.symbol,
        ts_exchange_ms: trade.trade_time_ms,
        ts_local_ms: now_ms(),
        price: parse_f64(&trade.price)?,
        qty: parse_f64(&trade.qty)?,
        aggressor_side: if trade.buyer_is_maker {
            AggressorSide::Sell
        } else {
            AggressorSide::Buy
        },
    })
}

fn parse_book_ticker(data: Value) -> anyhow::Result<TickerEvent> {
    let ticker: BinanceBookTicker = serde_json::from_value(data)?;
    Ok(TickerEvent {
        exchange: Exchange::Binance,
        symbol: ticker.symbol,
        ts_exchange_ms: ticker.event_time_ms.unwrap_or_else(now_ms),
        ts_local_ms: now_ms(),
        bid: parse_f64(&ticker.best_bid_price)?,
        ask: parse_f64(&ticker.best_ask_price)?,
        mark: None,
        index: None,
        funding_rate: None,
    })
}

fn parse_depth_delta(data: Value) -> anyhow::Result<DepthDeltaEvent> {
    let depth: BinanceDepthUpdate = serde_json::from_value(data)?;
    Ok(DepthDeltaEvent {
        exchange: Exchange::Binance,
        symbol: depth.symbol,
        ts_exchange_ms: depth.event_time_ms,
        ts_local_ms: now_ms(),
        first_update_id: Some(depth.first_update_id),
        sequence: Some(depth.final_update_id),
        previous_sequence: depth.previous_final_update_id,
        bids: parse_levels(depth.bids)?,
        asks: parse_levels(depth.asks)?,
    })
}

fn parse_levels(levels: Vec<[String; 2]>) -> anyhow::Result<Vec<DepthLevel>> {
    levels
        .into_iter()
        .map(|[price, qty]| {
            Ok(DepthLevel {
                price: parse_f64(&price)?,
                qty: parse_f64(&qty)?,
            })
        })
        .collect()
}

fn parse_f64(value: &str) -> anyhow::Result<f64> {
    Ok(value.parse::<f64>()?)
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Deserialize)]
struct CombinedMessage {
    stream: String,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

#[derive(Debug, Deserialize)]
struct BinanceBookTicker {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    best_bid_price: String,
    #[serde(rename = "a")]
    best_ask_price: String,
    #[serde(rename = "E")]
    event_time_ms: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct BinanceDepthUpdate {
    #[serde(rename = "E")]
    event_time_ms: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: u64,
    #[serde(rename = "u")]
    final_update_id: u64,
    #[serde(rename = "pu")]
    previous_final_update_id: Option<u64>,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_agg_trade() {
        let text = r#"{
          "stream":"btcusdt@aggTrade",
          "data":{"e":"aggTrade","E":1720000000000,"s":"BTCUSDT","a":1,"p":"64000.10","q":"0.002","f":1,"l":1,"T":1720000000010,"m":true}
        }"#;

        let events = parse_combined_message(text).expect("parse");
        let MarketEvent::Trade(trade) = &events[0] else {
            panic!("expected trade");
        };

        assert_eq!(trade.exchange, Exchange::Binance);
        assert_eq!(trade.symbol, "BTCUSDT");
        assert_eq!(trade.aggressor_side, AggressorSide::Sell);
        assert_eq!(trade.price, 64000.10);
        assert_eq!(trade.qty, 0.002);
    }

    #[test]
    fn parses_depth_delta() {
        let text = r#"{
          "stream":"btcusdt@depth@100ms",
          "data":{"e":"depthUpdate","E":1720000000000,"T":1720000000001,"s":"BTCUSDT","U":100,"u":102,"pu":99,"b":[["64000.00","1.25"]],"a":[["64001.00","0.50"]]}
        }"#;

        let events = parse_combined_message(text).expect("parse");
        let MarketEvent::DepthDelta(depth) = &events[0] else {
            panic!("expected depth");
        };

        assert_eq!(depth.sequence, Some(102));
        assert_eq!(depth.first_update_id, Some(100));
        assert_eq!(depth.previous_sequence, Some(99));
        assert_eq!(depth.bids[0].price, 64000.0);
        assert_eq!(depth.asks[0].qty, 0.50);
    }

    #[test]
    fn parses_book_ticker() {
        let text = r#"{
          "stream":"btcusdt@bookTicker",
          "data":{"e":"bookTicker","u":400900217,"s":"BTCUSDT","b":"64000.00","B":"0.10","a":"64000.10","A":"0.20","E":1720000000000,"T":1720000000001}
        }"#;

        let events = parse_combined_message(text).expect("parse");
        let MarketEvent::Ticker(ticker) = &events[0] else {
            panic!("expected ticker");
        };

        assert_eq!(ticker.bid, 64000.0);
        assert_eq!(ticker.ask, 64000.10);
    }
}
