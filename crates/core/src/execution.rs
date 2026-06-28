use crate::events::Exchange;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, Serialize)]
pub struct ExecutionStats {
    pub venue: Option<Exchange>,
    pub orders_submitted: u64,
    pub orders_accepted: u64,
    pub orders_rejected: u64,
    pub orders_cancelled: u64,
    pub open_orders: usize,
    pub reduce_only_stops: u64,
    pub reconciliations: u64,
    pub duplicate_client_order_ids: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOrderType {
    Market,
    Limit,
    StopMarket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOrderStatus {
    Accepted,
    Rejected,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct ExecutionOrderRequest {
    pub symbol: String,
    pub side: ExecutionSide,
    pub order_type: ExecutionOrderType,
    pub qty: f64,
    pub price: Option<f64>,
    pub stop_price: Option<f64>,
    pub reduce_only: bool,
    pub client_order_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionReport {
    pub exchange: Exchange,
    pub client_order_id: String,
    pub status: ExecutionOrderStatus,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesCredentials {
    pub api_key: String,
    pub secret_key: String,
}

#[derive(Debug, Clone)]
pub struct BinanceFuturesRestClient {
    http: reqwest::Client,
    base_url: String,
    credentials: BinanceFuturesCredentials,
}

impl BinanceFuturesRestClient {
    pub fn new(credentials: BinanceFuturesCredentials) -> Self {
        Self::with_base_url("https://fapi.binance.com", credentials)
    }

    pub fn with_base_url(
        base_url: impl Into<String>,
        credentials: BinanceFuturesCredentials,
    ) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            credentials,
        }
    }

    pub async fn place_order(
        &self,
        request: &ExecutionOrderRequest,
        timestamp_ms: i64,
    ) -> anyhow::Result<ExecutionReport> {
        let query = self.signed_order_query(request, timestamp_ms);
        let response = self
            .http
            .post(format!("{}/fapi/v1/order?{query}", self.base_url))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceOrderResponse>()
            .await?;

        Ok(ExecutionReport {
            exchange: Exchange::Binance,
            client_order_id: response.client_order_id,
            status: ExecutionOrderStatus::Accepted,
            reason: response.status,
        })
    }

    pub async fn cancel_order(
        &self,
        symbol: &str,
        client_order_id: &str,
        timestamp_ms: i64,
    ) -> anyhow::Result<ExecutionReport> {
        let mut params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("origClientOrderId".to_string(), client_order_id.to_string()),
            ("timestamp".to_string(), timestamp_ms.to_string()),
        ];
        let query = signed_query(&mut params, &self.credentials.secret_key);
        let response = self
            .http
            .delete(format!("{}/fapi/v1/order?{query}", self.base_url))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceOrderResponse>()
            .await?;

        Ok(ExecutionReport {
            exchange: Exchange::Binance,
            client_order_id: response.client_order_id,
            status: ExecutionOrderStatus::Cancelled,
            reason: response.status,
        })
    }

    pub async fn start_user_stream(&self) -> anyhow::Result<String> {
        let response = self
            .http
            .post(format!("{}/fapi/v1/listenKey", self.base_url))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?
            .error_for_status()?
            .json::<BinanceListenKeyResponse>()
            .await?;
        Ok(response.listen_key)
    }

    pub async fn keepalive_user_stream(&self, listen_key: &str) -> anyhow::Result<()> {
        self.http
            .put(format!(
                "{}/fapi/v1/listenKey?listenKey={listen_key}",
                self.base_url
            ))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub async fn close_user_stream(&self, listen_key: &str) -> anyhow::Result<()> {
        self.http
            .delete(format!(
                "{}/fapi/v1/listenKey?listenKey={listen_key}",
                self.base_url
            ))
            .header("X-MBX-APIKEY", &self.credentials.api_key)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    pub fn signed_order_query(&self, request: &ExecutionOrderRequest, timestamp_ms: i64) -> String {
        let mut params = order_params(request, timestamp_ms);
        signed_query(&mut params, &self.credentials.secret_key)
    }
}

pub fn parse_binance_user_stream_report(text: &str) -> anyhow::Result<Option<ExecutionReport>> {
    let message: BinanceUserStreamMessage = serde_json::from_str(text)?;
    if message.event_type != "ORDER_TRADE_UPDATE" {
        return Ok(None);
    }

    let Some(order) = message.order else {
        return Ok(None);
    };
    let status = match order.order_status.as_str() {
        "CANCELED" | "EXPIRED" => ExecutionOrderStatus::Cancelled,
        "REJECTED" => ExecutionOrderStatus::Rejected,
        _ => ExecutionOrderStatus::Accepted,
    };

    Ok(Some(ExecutionReport {
        exchange: Exchange::Binance,
        client_order_id: order.client_order_id,
        status,
        reason: order.order_status,
    }))
}

#[derive(Debug, Clone)]
pub struct BinanceDemoExecutionAdapter {
    stats: ExecutionStats,
    open_orders: BTreeMap<String, ExecutionOrderRequest>,
    next_sequence: u64,
}

impl Default for BinanceDemoExecutionAdapter {
    fn default() -> Self {
        Self::new()
    }
}

impl BinanceDemoExecutionAdapter {
    pub fn new() -> Self {
        Self {
            stats: ExecutionStats {
                venue: Some(Exchange::Binance),
                ..Default::default()
            },
            open_orders: BTreeMap::new(),
            next_sequence: 1,
        }
    }

    pub fn place_order(&mut self, mut request: ExecutionOrderRequest) -> ExecutionReport {
        self.stats.orders_submitted += 1;
        let client_order_id = request
            .client_order_id
            .clone()
            .unwrap_or_else(|| self.next_client_order_id(&request));
        request.client_order_id = Some(client_order_id.clone());

        if self.open_orders.contains_key(&client_order_id) {
            self.stats.duplicate_client_order_ids += 1;
            self.stats.orders_rejected += 1;
            return report(
                client_order_id,
                ExecutionOrderStatus::Rejected,
                "duplicate clientOrderId",
            );
        }

        if let Some(reason) = validate_order(&request) {
            self.stats.orders_rejected += 1;
            return report(client_order_id, ExecutionOrderStatus::Rejected, reason);
        }

        if request.reduce_only && matches!(request.order_type, ExecutionOrderType::StopMarket) {
            self.stats.reduce_only_stops += 1;
        }

        self.stats.orders_accepted += 1;
        self.open_orders.insert(client_order_id.clone(), request);
        self.stats.open_orders = self.open_orders.len();
        report(client_order_id, ExecutionOrderStatus::Accepted, "accepted")
    }

    pub fn cancel_order(&mut self, client_order_id: &str) -> ExecutionReport {
        if self.open_orders.remove(client_order_id).is_some() {
            self.stats.orders_cancelled += 1;
            self.stats.open_orders = self.open_orders.len();
            return report(
                client_order_id.to_string(),
                ExecutionOrderStatus::Cancelled,
                "cancelled",
            );
        }

        self.stats.orders_rejected += 1;
        report(
            client_order_id.to_string(),
            ExecutionOrderStatus::Rejected,
            "order not found",
        )
    }

    pub fn reconcile(&mut self, remote_open_ids: &[String]) {
        self.stats.reconciliations += 1;
        self.open_orders
            .retain(|client_order_id, _| remote_open_ids.contains(client_order_id));
        self.stats.open_orders = self.open_orders.len();
    }

    pub fn stats(&self) -> ExecutionStats {
        let mut stats = self.stats.clone();
        stats.open_orders = self.open_orders.len();
        stats
    }

    fn next_client_order_id(&mut self, request: &ExecutionOrderRequest) -> String {
        let id = format!(
            "scalper-{}-{}",
            request.symbol.to_ascii_lowercase(),
            self.next_sequence
        );
        self.next_sequence += 1;
        id
    }
}

fn validate_order(request: &ExecutionOrderRequest) -> Option<&'static str> {
    if request.symbol.is_empty() {
        return Some("missing symbol");
    }
    if request.qty <= 0.0 {
        return Some("invalid qty");
    }
    match request.order_type {
        ExecutionOrderType::Limit if request.price.is_none() => Some("limit price required"),
        ExecutionOrderType::StopMarket if request.stop_price.is_none() => {
            Some("stop price required")
        }
        _ => None,
    }
}

fn order_params(request: &ExecutionOrderRequest, timestamp_ms: i64) -> Vec<(String, String)> {
    let mut params = vec![
        ("symbol".to_string(), request.symbol.clone()),
        ("side".to_string(), binance_side(request.side).to_string()),
        (
            "type".to_string(),
            binance_order_type(request.order_type).to_string(),
        ),
        ("quantity".to_string(), format_decimal(request.qty)),
        (
            "newClientOrderId".to_string(),
            request.client_order_id.clone().unwrap_or_default(),
        ),
        ("timestamp".to_string(), timestamp_ms.to_string()),
    ];

    if let Some(price) = request.price {
        params.push(("price".to_string(), format_decimal(price)));
        params.push(("timeInForce".to_string(), "GTC".to_string()));
    }
    if let Some(stop_price) = request.stop_price {
        params.push(("stopPrice".to_string(), format_decimal(stop_price)));
    }
    if request.reduce_only {
        params.push(("reduceOnly".to_string(), "true".to_string()));
    }

    params.retain(|(_, value)| !value.is_empty());
    params
}

fn signed_query(params: &mut [(String, String)], secret_key: &str) -> String {
    let query = params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");
    let signature = hmac_sha256_hex(secret_key, &query);
    format!("{query}&signature={signature}")
}

fn hmac_sha256_hex(secret_key: &str, payload: &str) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret_key.as_bytes()).expect("HMAC accepts any key length");
    mac.update(payload.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn binance_side(side: ExecutionSide) -> &'static str {
    match side {
        ExecutionSide::Buy => "BUY",
        ExecutionSide::Sell => "SELL",
    }
}

fn binance_order_type(order_type: ExecutionOrderType) -> &'static str {
    match order_type {
        ExecutionOrderType::Market => "MARKET",
        ExecutionOrderType::Limit => "LIMIT",
        ExecutionOrderType::StopMarket => "STOP_MARKET",
    }
}

fn format_decimal(value: f64) -> String {
    let formatted = format!("{value:.8}");
    formatted
        .trim_end_matches('0')
        .trim_end_matches('.')
        .to_string()
}

fn report(
    client_order_id: String,
    status: ExecutionOrderStatus,
    reason: &'static str,
) -> ExecutionReport {
    ExecutionReport {
        exchange: Exchange::Binance,
        client_order_id,
        status,
        reason: reason.to_string(),
    }
}

#[derive(Debug, Deserialize)]
struct BinanceOrderResponse {
    #[serde(rename = "clientOrderId")]
    client_order_id: String,
    status: String,
}

#[derive(Debug, Deserialize)]
struct BinanceListenKeyResponse {
    #[serde(rename = "listenKey")]
    listen_key: String,
}

#[derive(Debug, Deserialize)]
struct BinanceUserStreamMessage {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "o")]
    order: Option<BinanceOrderUpdate>,
}

#[derive(Debug, Deserialize)]
struct BinanceOrderUpdate {
    #[serde(rename = "c")]
    client_order_id: String,
    #[serde(rename = "X")]
    order_status: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_limit_order_with_generated_id() {
        let mut adapter = BinanceDemoExecutionAdapter::new();
        let report = adapter.place_order(limit_order(None));

        assert_eq!(report.status, ExecutionOrderStatus::Accepted);
        assert!(report.client_order_id.starts_with("scalper-btcusdt-"));
        assert_eq!(adapter.stats().open_orders, 1);
    }

    #[test]
    fn rejects_duplicate_client_order_id() {
        let mut adapter = BinanceDemoExecutionAdapter::new();
        adapter.place_order(limit_order(Some("dup".to_string())));
        let report = adapter.place_order(limit_order(Some("dup".to_string())));

        assert_eq!(report.status, ExecutionOrderStatus::Rejected);
        assert_eq!(adapter.stats().duplicate_client_order_ids, 1);
    }

    #[test]
    fn tracks_reduce_only_stop_and_cancel() {
        let mut adapter = BinanceDemoExecutionAdapter::new();
        let report = adapter.place_order(ExecutionOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: ExecutionSide::Sell,
            order_type: ExecutionOrderType::StopMarket,
            qty: 0.01,
            price: None,
            stop_price: Some(90_000.0),
            reduce_only: true,
            client_order_id: Some("stop-1".to_string()),
        });

        assert_eq!(report.status, ExecutionOrderStatus::Accepted);
        assert_eq!(adapter.stats().reduce_only_stops, 1);
        let cancel = adapter.cancel_order("stop-1");
        assert_eq!(cancel.status, ExecutionOrderStatus::Cancelled);
        assert_eq!(adapter.stats().open_orders, 0);
    }

    #[test]
    fn reconcile_removes_missing_remote_order() {
        let mut adapter = BinanceDemoExecutionAdapter::new();
        adapter.place_order(limit_order(Some("local".to_string())));
        adapter.reconcile(&[]);

        assert_eq!(adapter.stats().reconciliations, 1);
        assert_eq!(adapter.stats().open_orders, 0);
    }

    #[test]
    fn builds_signed_order_query() {
        let client = BinanceFuturesRestClient::with_base_url(
            "https://example.test",
            BinanceFuturesCredentials {
                api_key: "key".to_string(),
                secret_key: "secret".to_string(),
            },
        );
        let query = client.signed_order_query(&limit_order(Some("id-1".to_string())), 123);

        assert!(query.contains("symbol=BTCUSDT"));
        assert!(query.contains("side=BUY"));
        assert!(query.contains("type=LIMIT"));
        assert!(query.contains("timeInForce=GTC"));
        assert!(query.contains("newClientOrderId=id-1"));
        assert!(query.contains("timestamp=123"));
        assert!(query.contains("signature="));
    }

    #[test]
    fn parses_user_stream_order_update() {
        let text = r#"{
          "e":"ORDER_TRADE_UPDATE",
          "E":1568879465651,
          "o":{"s":"BTCUSDT","c":"my-id","S":"BUY","o":"LIMIT","X":"CANCELED"}
        }"#;

        let report = parse_binance_user_stream_report(text)
            .expect("parse")
            .expect("report");
        assert_eq!(report.client_order_id, "my-id");
        assert_eq!(report.status, ExecutionOrderStatus::Cancelled);
    }

    fn limit_order(client_order_id: Option<String>) -> ExecutionOrderRequest {
        ExecutionOrderRequest {
            symbol: "BTCUSDT".to_string(),
            side: ExecutionSide::Buy,
            order_type: ExecutionOrderType::Limit,
            qty: 0.01,
            price: Some(100_000.0),
            stop_price: None,
            reduce_only: false,
            client_order_id,
        }
    }
}
