use crate::events::{AggressorSide, Exchange, KlineEvent, MarketEvent, TradeEvent};
use reqwest::StatusCode;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

const BINANCE_USD_M_REST_BASE: &str = "https://fapi.binance.com";
const MAX_KLINES_LIMIT: usize = 1500;

#[derive(Debug, Clone)]
pub struct BinanceKlineImportConfig {
    pub symbol: String,
    pub interval: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
}

#[derive(Debug, Clone)]
pub struct BinanceAggTradeImportConfig {
    pub symbol: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
}

pub async fn fetch_binance_klines(
    config: BinanceKlineImportConfig,
) -> anyhow::Result<Vec<MarketEvent>> {
    let client = reqwest::Client::new();
    let mut start = config.start_time_ms;
    let mut events = Vec::new();

    while start < config.end_time_ms {
        let batch = fetch_klines_batch(
            &client,
            &config.symbol,
            &config.interval,
            start,
            config.end_time_ms,
        )
        .await?;
        if batch.is_empty() {
            break;
        }

        let mut last_close = start;
        for kline in batch {
            last_close = kline.6;
            events.push(MarketEvent::Kline(
                kline.into_event(&config.symbol, &config.interval)?,
            ));
        }
        start = last_close.saturating_add(1);
    }

    Ok(events)
}

pub async fn fetch_binance_agg_trades(
    config: BinanceAggTradeImportConfig,
) -> anyhow::Result<Vec<MarketEvent>> {
    let client = reqwest::Client::new();
    let mut start = config.start_time_ms;
    let mut events = Vec::new();

    while start < config.end_time_ms {
        let batch =
            fetch_agg_trades_batch(&client, &config.symbol, start, config.end_time_ms).await?;
        if batch.is_empty() {
            break;
        }

        let mut last_trade_time = start;
        for trade in batch {
            last_trade_time = trade.trade_time_ms;
            events.push(MarketEvent::Trade(trade.into_event(&config.symbol)?));
        }
        let next_start = last_trade_time.saturating_add(1);
        if next_start <= start {
            break;
        }
        start = next_start;
        sleep(Duration::from_millis(250)).await;
    }

    Ok(events)
}

async fn fetch_klines_batch(
    client: &reqwest::Client,
    symbol: &str,
    interval: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> anyhow::Result<Vec<BinanceKline>> {
    let url = format!("{BINANCE_USD_M_REST_BASE}/fapi/v1/klines");
    let response = client
        .get(url)
        .query(&[
            ("symbol", symbol),
            ("interval", interval),
            ("startTime", &start_time_ms.to_string()),
            ("endTime", &end_time_ms.to_string()),
            ("limit", &MAX_KLINES_LIMIT.to_string()),
        ])
        .send()
        .await?
        .error_for_status()?
        .json::<Vec<BinanceKline>>()
        .await?;
    Ok(response)
}

async fn fetch_agg_trades_batch(
    client: &reqwest::Client,
    symbol: &str,
    start_time_ms: i64,
    end_time_ms: i64,
) -> anyhow::Result<Vec<BinanceAggTrade>> {
    let url = format!("{BINANCE_USD_M_REST_BASE}/fapi/v1/aggTrades");
    for attempt in 0..3 {
        let response = client
            .get(&url)
            .query(&[
                ("symbol", symbol),
                ("startTime", &start_time_ms.to_string()),
                ("endTime", &end_time_ms.to_string()),
                ("limit", "1000"),
            ])
            .send()
            .await?;
        if response.status() == StatusCode::TOO_MANY_REQUESTS {
            sleep(Duration::from_secs(30 * (attempt + 1))).await;
            continue;
        }

        return Ok(response
            .error_for_status()?
            .json::<Vec<BinanceAggTrade>>()
            .await?);
    }

    anyhow::bail!("binance aggTrades rate limit persisted after retries")
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct BinanceKline(
    i64,
    String,
    String,
    String,
    String,
    String,
    i64,
    String,
    u64,
    String,
    String,
    String,
);

impl BinanceKline {
    fn into_event(self, symbol: &str, interval: &str) -> anyhow::Result<KlineEvent> {
        Ok(KlineEvent {
            exchange: Exchange::Binance,
            symbol: symbol.to_string(),
            interval: interval.to_string(),
            open_time_ms: self.0,
            close_time_ms: self.6,
            ts_local_ms: self.6,
            open: self.1.parse()?,
            high: self.2.parse()?,
            low: self.3.parse()?,
            close: self.4.parse()?,
            volume: self.5.parse()?,
            quote_volume: self.7.parse()?,
            trades: self.8,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
struct BinanceAggTrade {
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    qty: String,
    #[serde(rename = "T")]
    trade_time_ms: i64,
    #[serde(rename = "m")]
    buyer_is_maker: bool,
}

impl BinanceAggTrade {
    fn into_event(self, symbol: &str) -> anyhow::Result<TradeEvent> {
        Ok(TradeEvent {
            exchange: Exchange::Binance,
            symbol: symbol.to_string(),
            ts_exchange_ms: self.trade_time_ms,
            ts_local_ms: self.trade_time_ms,
            price: self.price.parse()?,
            qty: self.qty.parse()?,
            aggressor_side: if self.buyer_is_maker {
                AggressorSide::Sell
            } else {
                AggressorSide::Buy
            },
        })
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn default_kline_run_name(symbol: &str, interval: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "klines-{}-{}-{}-{}-{}",
        symbol.to_ascii_lowercase(),
        interval,
        start_ms,
        end_ms,
        now_ms()
    )
}

pub fn default_agg_trades_run_name(symbol: &str, start_ms: i64, end_ms: i64) -> String {
    format!(
        "aggtrades-{}-{}-{}-{}",
        symbol.to_ascii_lowercase(),
        start_ms,
        end_ms,
        now_ms()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_binance_kline_to_event() {
        let kline = BinanceKline(
            1,
            "100.0".to_string(),
            "110.0".to_string(),
            "90.0".to_string(),
            "105.0".to_string(),
            "2.5".to_string(),
            60_000,
            "260.0".to_string(),
            42,
            "1.0".to_string(),
            "105.0".to_string(),
            "0".to_string(),
        );

        let event = kline.into_event("BTCUSDT", "1m").expect("kline");

        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.interval, "1m");
        assert_eq!(event.close, 105.0);
        assert_eq!(event.trades, 42);
    }

    #[test]
    fn converts_binance_agg_trade_to_trade_event() {
        let trade = BinanceAggTrade {
            price: "100.5".to_string(),
            qty: "0.25".to_string(),
            trade_time_ms: 42,
            buyer_is_maker: true,
        };

        let event = trade.into_event("BTCUSDT").expect("agg trade");

        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.price, 100.5);
        assert_eq!(event.qty, 0.25);
        assert_eq!(event.aggressor_side, AggressorSide::Sell);
    }
}
