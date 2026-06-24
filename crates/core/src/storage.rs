use crate::events::{DepthDeltaEvent, Exchange, MarketEvent, TickerEvent, TradeEvent};
use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::Utc;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
    file::properties::WriterProperties,
};
use serde::Serialize;
use std::{
    fs::{self, File},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch},
    time::{MissedTickBehavior, interval},
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

#[derive(Debug, Clone, Serialize)]
pub struct StorageStats {
    pub enabled: bool,
    pub current_file: Option<PathBuf>,
    pub buffered_events: usize,
    pub records_written: u64,
    pub batches_written: u64,
    pub flush_errors: u64,
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            enabled: false,
            current_file: None,
            buffered_events: 0,
            records_written: 0,
            batches_written: 0,
            flush_errors: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub enabled: bool,
    pub root_dir: PathBuf,
    pub symbol: String,
    pub flush_batch_size: usize,
    pub flush_interval: Duration,
}

pub fn spawn_parquet_storage(
    config: StorageConfig,
    mut storage_rx: mpsc::Receiver<MarketEvent>,
    stats_tx: watch::Sender<StorageStats>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if !config.enabled {
            let _ = stats_tx.send(StorageStats::default());
            return;
        }

        let mut writer = match ParquetEventWriter::new(&config) {
            Ok(writer) => writer,
            Err(error) => {
                warn!(%error, "failed to start parquet storage");
                return;
            }
        };

        let _ = stats_tx.send(writer.stats());

        let mut flush_tick = interval(config.flush_interval);
        flush_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    if let Err(error) = writer.flush() {
                        warn!(%error, "final parquet flush failed");
                    }
                    if let Err(error) = writer.close() {
                        warn!(%error, "parquet close failed");
                    }
                    let _ = stats_tx.send(writer.stats());
                    return;
                }
                _ = flush_tick.tick() => {
                    if writer.has_buffered_events() {
                        if let Err(error) = writer.flush() {
                            warn!(%error, "parquet flush failed");
                        }
                        let _ = stats_tx.send(writer.stats());
                    }
                }
                event = storage_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    if let Err(error) = writer.push(event) {
                        warn!(%error, "failed to buffer storage event");
                    }

                    if writer.buffered_events() >= config.flush_batch_size
                        && let Err(error) = writer.flush()
                    {
                        warn!(%error, "parquet batch flush failed");
                    }

                    let _ = stats_tx.send(writer.stats());
                }
            }
        }
    })
}

pub fn spawn_parquet_replay(
    input_path: PathBuf,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: CancellationToken,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let path_for_read = input_path.clone();
        let events = match tokio::task::spawn_blocking(move || {
            read_events_from_parquet(path_for_read)
        })
        .await
        {
            Ok(Ok(events)) => events,
            Ok(Err(error)) => {
                warn!(%error, path = %input_path.display(), "failed to read replay parquet");
                return;
            }
            Err(error) => {
                warn!(%error, "replay reader task failed");
                return;
            }
        };

        info!(path = %input_path.display(), events = events.len(), "starting parquet replay");

        for event in events {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                sent = market_tx.send(event) => {
                    if sent.is_err() {
                        return;
                    }
                }
            }
        }

        info!(path = %input_path.display(), "parquet replay finished");
    })
}

pub fn read_events_from_parquet(path: impl AsRef<Path>) -> anyhow::Result<Vec<MarketEvent>> {
    let path = path.as_ref();
    let mut events = if path.is_dir() {
        let mut files = fs::read_dir(path)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "parquet"))
            .collect::<Vec<_>>();
        files.sort();

        let mut events = Vec::new();
        for file in files {
            events.extend(read_events_from_parquet_file(&file)?);
        }
        events
    } else {
        read_events_from_parquet_file(path)?
    };

    events.sort_by_key(event_ts_local_ms);
    Ok(events)
}

fn read_events_from_parquet_file(path: &Path) -> anyhow::Result<Vec<MarketEvent>> {
    let file = File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut events = Vec::new();

    for batch in reader {
        let batch = batch?;
        let payload_json = string_column(&batch, "payload_json")?;

        for row in 0..batch.num_rows() {
            if payload_json.is_null(row) {
                continue;
            }
            let event = serde_json::from_str::<MarketEvent>(payload_json.value(row))?;
            events.push(event);
        }
    }

    Ok(events)
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> anyhow::Result<&'a StringArray> {
    batch
        .column_by_name(name)
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| anyhow::anyhow!("missing utf8 column {name}"))
}

struct ParquetEventWriter {
    schema: Arc<Schema>,
    run_dir: PathBuf,
    current_file: Option<PathBuf>,
    buffer: Vec<StoredEvent>,
    records_written: u64,
    batches_written: u64,
    flush_errors: u64,
}

impl ParquetEventWriter {
    fn new(config: &StorageConfig) -> anyhow::Result<Self> {
        let run_dir = run_dir(&config.root_dir, &config.symbol);
        fs::create_dir_all(&run_dir)?;

        let schema = Arc::new(storage_schema());

        info!(path = %run_dir.display(), "parquet storage started");

        Ok(Self {
            schema,
            run_dir,
            current_file: None,
            buffer: Vec::with_capacity(config.flush_batch_size),
            records_written: 0,
            batches_written: 0,
            flush_errors: 0,
        })
    }

    fn push(&mut self, event: MarketEvent) -> anyhow::Result<()> {
        self.buffer.push(StoredEvent::from_market_event(event)?);
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        let batch = build_batch(self.schema.clone(), &self.buffer)?;
        let count = self.buffer.len() as u64;
        let next_batch = self.batches_written + 1;
        let file_path = self.run_dir.join(format!("events-{next_batch:06}.parquet"));
        let file = File::create(&file_path)?;
        let props = WriterProperties::builder().build();
        let mut writer = ArrowWriter::try_new(file, self.schema.clone(), Some(props))?;

        writer.write(&batch)?;
        writer.close()?;

        self.current_file = Some(file_path);
        self.buffer.clear();
        self.records_written += count;
        self.batches_written = next_batch;
        Ok(())
    }

    fn close(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    fn has_buffered_events(&self) -> bool {
        !self.buffer.is_empty()
    }

    fn buffered_events(&self) -> usize {
        self.buffer.len()
    }

    fn stats(&self) -> StorageStats {
        StorageStats {
            enabled: true,
            current_file: self.current_file.clone(),
            buffered_events: self.buffer.len(),
            records_written: self.records_written,
            batches_written: self.batches_written,
            flush_errors: self.flush_errors,
        }
    }
}

#[derive(Debug)]
struct StoredEvent {
    ts_local_ms: i64,
    ts_exchange_ms: Option<i64>,
    exchange: String,
    symbol: String,
    event_kind: String,
    payload_json: String,
}

impl StoredEvent {
    fn from_market_event(event: MarketEvent) -> anyhow::Result<Self> {
        Ok(Self {
            ts_local_ms: event_ts_local_ms(&event),
            ts_exchange_ms: event_ts_exchange_ms(&event),
            exchange: event_exchange(&event),
            symbol: event_symbol(&event),
            event_kind: event_kind(&event).to_string(),
            payload_json: serde_json::to_string(&event)?,
        })
    }
}

fn storage_schema() -> Schema {
    Schema::new(vec![
        Field::new("ts_local_ms", DataType::Int64, false),
        Field::new("ts_exchange_ms", DataType::Int64, true),
        Field::new("exchange", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("event_kind", DataType::Utf8, false),
        Field::new("payload_json", DataType::Utf8, false),
    ])
}

fn build_batch(schema: Arc<Schema>, events: &[StoredEvent]) -> anyhow::Result<RecordBatch> {
    let ts_local_ms: ArrayRef = Arc::new(Int64Array::from(
        events
            .iter()
            .map(|event| event.ts_local_ms)
            .collect::<Vec<_>>(),
    ));
    let ts_exchange_ms: ArrayRef = Arc::new(Int64Array::from(
        events
            .iter()
            .map(|event| event.ts_exchange_ms)
            .collect::<Vec<_>>(),
    ));
    let exchange: ArrayRef = Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.exchange.as_str())
            .collect::<Vec<_>>(),
    ));
    let symbol: ArrayRef = Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.symbol.as_str())
            .collect::<Vec<_>>(),
    ));
    let event_kind: ArrayRef = Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.event_kind.as_str())
            .collect::<Vec<_>>(),
    ));
    let payload_json: ArrayRef = Arc::new(StringArray::from(
        events
            .iter()
            .map(|event| event.payload_json.as_str())
            .collect::<Vec<_>>(),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            ts_local_ms,
            ts_exchange_ms,
            exchange,
            symbol,
            event_kind,
            payload_json,
        ],
    )?)
}

fn event_ts_local_ms(event: &MarketEvent) -> i64 {
    match event {
        MarketEvent::Trade(TradeEvent { ts_local_ms, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { ts_local_ms, .. })
        | MarketEvent::Ticker(TickerEvent { ts_local_ms, .. })
        | MarketEvent::Heartbeat { ts_local_ms, .. } => *ts_local_ms,
    }
}

fn event_ts_exchange_ms(event: &MarketEvent) -> Option<i64> {
    match event {
        MarketEvent::Trade(TradeEvent { ts_exchange_ms, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { ts_exchange_ms, .. })
        | MarketEvent::Ticker(TickerEvent { ts_exchange_ms, .. }) => Some(*ts_exchange_ms),
        MarketEvent::Heartbeat { .. } => None,
    }
}

fn event_exchange(event: &MarketEvent) -> String {
    match event {
        MarketEvent::Trade(TradeEvent { exchange, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { exchange, .. })
        | MarketEvent::Ticker(TickerEvent { exchange, .. }) => exchange_name(*exchange).to_string(),
        MarketEvent::Heartbeat { .. } => "internal".to_string(),
    }
}

fn event_symbol(event: &MarketEvent) -> String {
    match event {
        MarketEvent::Trade(TradeEvent { symbol, .. })
        | MarketEvent::DepthDelta(DepthDeltaEvent { symbol, .. })
        | MarketEvent::Ticker(TickerEvent { symbol, .. }) => symbol.clone(),
        MarketEvent::Heartbeat { .. } => String::new(),
    }
}

fn event_kind(event: &MarketEvent) -> &'static str {
    match event {
        MarketEvent::Trade(_) => "trade",
        MarketEvent::DepthDelta(_) => "depth_delta",
        MarketEvent::Ticker(_) => "ticker",
        MarketEvent::Heartbeat { .. } => "heartbeat",
    }
}

fn exchange_name(exchange: Exchange) -> &'static str {
    match exchange {
        Exchange::Binance => "binance",
        Exchange::Mexc => "mexc",
        Exchange::Deribit => "deribit",
        Exchange::Okx => "okx",
        Exchange::Bybit => "bybit",
        Exchange::Hyperliquid => "hyperliquid",
    }
}

fn run_dir(root: &Path, symbol: &str) -> PathBuf {
    let now = Utc::now();
    root.join(now.format("%Y-%m-%d").to_string())
        .join(symbol.to_ascii_lowercase())
        .join(format!("run-{}", now.format("%H%M%S")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::AggressorSide;

    #[test]
    fn stored_event_extracts_trade_metadata() {
        let stored = StoredEvent::from_market_event(MarketEvent::Trade(TradeEvent {
            exchange: Exchange::Binance,
            symbol: "BTCUSDT".to_string(),
            ts_exchange_ms: 10,
            ts_local_ms: 20,
            price: 100.0,
            qty: 0.1,
            aggressor_side: AggressorSide::Buy,
        }))
        .expect("stored event");

        assert_eq!(stored.ts_local_ms, 20);
        assert_eq!(stored.ts_exchange_ms, Some(10));
        assert_eq!(stored.exchange, "binance");
        assert_eq!(stored.symbol, "BTCUSDT");
        assert_eq!(stored.event_kind, "trade");
        assert!(stored.payload_json.contains("trade"));
    }

    #[test]
    fn builds_record_batch() {
        let events = vec![StoredEvent {
            ts_local_ms: 1,
            ts_exchange_ms: Some(2),
            exchange: "binance".to_string(),
            symbol: "BTCUSDT".to_string(),
            event_kind: "ticker".to_string(),
            payload_json: "{}".to_string(),
        }];

        let batch = build_batch(Arc::new(storage_schema()), &events).expect("batch");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 6);
    }

    #[test]
    fn parquet_roundtrip_reads_events() {
        let root = std::env::temp_dir().join(format!(
            "scalper-storage-test-{}",
            Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));

        let config = StorageConfig {
            enabled: true,
            root_dir: root.clone(),
            symbol: "BTCUSDT".to_string(),
            flush_batch_size: 10,
            flush_interval: Duration::from_secs(1),
        };

        let mut writer = ParquetEventWriter::new(&config).expect("writer");
        writer
            .push(MarketEvent::Heartbeat {
                component: "test".to_string(),
                ts_local_ms: 2,
            })
            .expect("push heartbeat");
        writer
            .push(MarketEvent::Trade(TradeEvent {
                exchange: Exchange::Binance,
                symbol: "BTCUSDT".to_string(),
                ts_exchange_ms: 1,
                ts_local_ms: 1,
                price: 100.0,
                qty: 0.1,
                aggressor_side: AggressorSide::Buy,
            }))
            .expect("push trade");
        writer.flush().expect("flush");
        writer.close().expect("close");

        let file_path = writer.current_file.clone().expect("current file");
        let events = read_events_from_parquet(&file_path).expect("read events");
        assert_eq!(events.len(), 2);
        assert!(matches!(events[0], MarketEvent::Trade(_)));
        assert!(matches!(events[1], MarketEvent::Heartbeat { .. }));

        let _ = fs::remove_dir_all(root);
    }
}
