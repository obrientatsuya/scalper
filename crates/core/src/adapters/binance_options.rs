use crate::events::{Exchange, MarketEvent, OptionGreekEvent, OptionType};
use chrono::{NaiveDate, TimeZone, Utc};
use serde::Deserialize;
use std::{collections::BTreeMap, time::Duration};
use tokio::{sync::mpsc, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::warn;

const BINANCE_OPTIONS_BASE: &str = "https://eapi.binance.com";

#[derive(Debug, Clone)]
pub struct BinanceOptionsClient {
    http: reqwest::Client,
    base_url: String,
}

impl Default for BinanceOptionsClient {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceOptionsClient {
    pub fn new() -> Self {
        Self::with_base_url(BINANCE_OPTIONS_BASE)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
        }
    }

    pub async fn fetch_option_greeks(
        &self,
        underlying: &str,
        expiration: Option<&str>,
    ) -> anyhow::Result<Vec<OptionGreekEvent>> {
        let marks = self.fetch_marks(None).await?;
        let open_interest = if let Some(expiration) = expiration {
            self.fetch_open_interest(underlying, expiration).await?
        } else {
            BTreeMap::new()
        };

        Ok(normalize_option_chain(
            underlying,
            marks,
            open_interest,
            now_ms(),
        ))
    }

    pub async fn fetch_marks(
        &self,
        symbol: Option<&str>,
    ) -> anyhow::Result<Vec<BinanceOptionMark>> {
        let url = match symbol {
            Some(symbol) => format!("{}/eapi/v1/mark?symbol={symbol}", self.base_url),
            None => format!("{}/eapi/v1/mark", self.base_url),
        };
        Ok(self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?)
    }

    pub async fn fetch_open_interest(
        &self,
        underlying: &str,
        expiration: &str,
    ) -> anyhow::Result<BTreeMap<String, f64>> {
        let url = format!(
            "{}/eapi/v1/openInterest?underlyingAsset={underlying}&expiration={expiration}",
            self.base_url
        );
        let rows = self
            .http
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json::<Vec<BinanceOptionOpenInterest>>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.symbol, row.open_interest))
            .collect())
    }
}

pub fn spawn_binance_options_greeks(
    underlying: String,
    expiration: Option<String>,
    interval: Duration,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let client = BinanceOptionsClient::new();
        let mut tick = tokio::time::interval(interval);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = tick.tick() => {
                    match client.fetch_option_greeks(&underlying, expiration.as_deref()).await {
                        Ok(events) => {
                            for event in events {
                                if market_tx.send(MarketEvent::OptionGreek(event)).await.is_err() {
                                    return;
                                }
                            }
                        }
                        Err(error) => {
                            warn!(%error, underlying, "binance options greeks fetch failed");
                        }
                    }
                }
            }
        }
    })
}

pub fn normalize_option_chain(
    underlying: &str,
    marks: Vec<BinanceOptionMark>,
    open_interest: BTreeMap<String, f64>,
    ts_local_ms: i64,
) -> Vec<OptionGreekEvent> {
    marks
        .into_iter()
        .filter_map(|mark| {
            let parsed = parse_option_symbol(&mark.symbol)?;
            if parsed.underlying != underlying {
                return None;
            }
            Some(OptionGreekEvent {
                exchange: Exchange::Binance,
                underlying: format!("{}USDT", parsed.underlying),
                option_symbol: mark.symbol.clone(),
                expiry_ms: parsed.expiry_ms,
                strike: parsed.strike,
                option_type: parsed.option_type,
                ts_local_ms,
                mark_price: mark.mark_price,
                open_interest_contracts: open_interest
                    .get(&mark.symbol)
                    .copied()
                    .unwrap_or_default(),
                contract_unit: 1.0,
                gamma: mark.gamma,
                delta: Some(mark.delta),
                iv: Some(mark.mark_iv),
            })
        })
        .collect()
}

fn parse_option_symbol(symbol: &str) -> Option<ParsedOptionSymbol> {
    let parts = symbol.split('-').collect::<Vec<_>>();
    if parts.len() != 4 {
        return None;
    }

    let date = NaiveDate::parse_from_str(parts[1], "%y%m%d").ok()?;
    let expiry_ms = Utc
        .from_utc_datetime(&date.and_hms_opt(8, 0, 0)?)
        .timestamp_millis();
    let strike = parts[2].parse().ok()?;
    let option_type = match parts[3] {
        "C" => OptionType::Call,
        "P" => OptionType::Put,
        _ => return None,
    };

    Some(ParsedOptionSymbol {
        underlying: parts[0].to_string(),
        expiry_ms,
        strike,
        option_type,
    })
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Debug, Clone)]
struct ParsedOptionSymbol {
    underlying: String,
    expiry_ms: i64,
    strike: f64,
    option_type: OptionType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceOptionMark {
    pub symbol: String,
    #[serde(rename = "markPrice", deserialize_with = "de_string_f64")]
    pub mark_price: f64,
    #[serde(rename = "markIV", deserialize_with = "de_string_f64")]
    pub mark_iv: f64,
    #[serde(deserialize_with = "de_string_f64")]
    pub delta: f64,
    #[serde(deserialize_with = "de_string_f64")]
    pub gamma: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BinanceOptionOpenInterest {
    pub symbol: String,
    #[serde(
        rename = "sumOpenInterest",
        alias = "openInterest",
        deserialize_with = "de_string_f64"
    )]
    pub open_interest: f64,
}

fn de_string_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringOrFloat {
        String(String),
        Float(f64),
    }

    match StringOrFloat::deserialize(deserializer)? {
        StringOrFloat::String(value) => value.parse().map_err(serde::de::Error::custom),
        StringOrFloat::Float(value) => Ok(value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_option_symbol() {
        let parsed = parse_option_symbol("BTC-260626-90000-C").expect("symbol");

        assert_eq!(parsed.underlying, "BTC");
        assert_eq!(parsed.strike, 90_000.0);
        assert_eq!(parsed.option_type, OptionType::Call);
        assert!(parsed.expiry_ms > 0);
    }

    #[test]
    fn normalizes_marks_and_open_interest_to_option_greeks() {
        let marks = vec![
            BinanceOptionMark {
                symbol: "BTC-260626-90000-C".to_string(),
                mark_price: 100.0,
                mark_iv: 0.5,
                delta: 0.4,
                gamma: 0.0001,
            },
            BinanceOptionMark {
                symbol: "ETH-260626-4000-C".to_string(),
                mark_price: 10.0,
                mark_iv: 0.7,
                delta: 0.5,
                gamma: 0.001,
            },
        ];
        let open_interest = BTreeMap::from([("BTC-260626-90000-C".to_string(), 25.0)]);

        let events = normalize_option_chain("BTC", marks, open_interest, 1);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].underlying, "BTCUSDT");
        assert_eq!(events[0].strike, 90_000.0);
        assert_eq!(events[0].open_interest_contracts, 25.0);
        assert_eq!(events[0].gamma, 0.0001);
    }

    #[test]
    fn deserializes_binance_mark_payload() {
        let mark: BinanceOptionMark = serde_json::from_str(
            r#"{"symbol":"BTC-260626-90000-C","markPrice":"100.1","markIV":"0.5","delta":"0.4","gamma":"0.0001"}"#,
        )
        .expect("mark");

        assert_eq!(mark.mark_price, 100.1);
        assert_eq!(mark.gamma, 0.0001);
    }
}
