# Implementation Roadmap

Current code starts Setor A from `scalping-amt-gex-orderflow-spec.md`.

## Done

- Rust workspace.
- `scalper-core` crate.
- `scalper-app` binary.
- Config loader with env override prefix `SCALPER__`.
- Runtime states.
- Normalized market event types.
- Supervisor loop.
- Stub market heartbeat.
- Health endpoint at `/health`.
- Binance USD-M market data adapter:
  - combined WebSocket stream;
  - `aggTrade`;
  - `bookTicker`;
  - `depth@100ms`;
  - reconnect loop;
  - normalized `Trade`, `Ticker`, `DepthDelta` events.
- Binance local orderbook:
  - REST snapshot `/fapi/v1/depth`;
  - diff sequencing with `U/u/pu`;
  - stale buffered delta discard;
  - best bid/ask stats in `/health`;
  - resync/gap counters.
- Raw Parquet storage:
  - `data/raw/YYYY-MM-DD/<symbol>/run-HHMMSS/events-000001.parquet`;
  - generic replay-ready schema;
  - batched flush by size/interval;
  - each flush closes a valid Parquet file;
  - storage stats in `/health`.
- Deterministic replay base:
  - reads `events.parquet`;
  - reconstructs `MarketEvent`;
  - sorts by `ts_local_ms`;
  - feeds same app pipeline;
  - disables live venue connection in replay mode.

## Next

1. Add recorded orderbook snapshots for full orderbook replay.
2. Add latency metrics: event lag, ws jitter, queue depth.
3. Add orderflow/CVD windows.
4. Add heatmap/orderbook feature snapshots.
5. Add MEXC adapter after Binance path is stable.
