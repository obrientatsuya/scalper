# Project Memory

## 2026-06-24 Scalper Decisions

- Build automated 30s BTCUSDT scalping research/execution system in Rust + Tokio.
- No visual dependency: no Bookmap/MMT runtime, no screen scraping.
- Bookmap/MMT are conceptual feature references only.
- Pipeline must be data-driven: public API/feed, capture, replay, backtest.
- Binance USD-M is base venue for market data and first orderbook.
- MEXC Futures is candidate later after API, fees, risk, latency, and private stream validation.
- 500x leverage is paper/stress-test only until proven. Live starts capped, initial hard cap 20x.
- Leverage must be computed by risk engine from stop distance, fees, slippage, and liquidation buffer.
- No live trading before replay, walk-forward, ablation, paper, and positive expectancy after costs.

Implemented:

- Rust workspace.
- `scalper-core` and `scalper-app`.
- Config loader.
- Runtime states.
- Supervisor loop.
- Health endpoint.
- Binance combined WebSocket market data adapter: `aggTrade`, `bookTicker`, `depth@100ms`.
- Binance local orderbook with REST snapshot and `U/u/pu` diff sequencing.
- Orderbook stats in `/health`.
- Raw Parquet storage:
  - writes closed batch files like `events-000001.parquet`;
  - schema: `ts_local_ms`, `ts_exchange_ms`, `exchange`, `symbol`, `event_kind`, `payload_json`;
  - storage stats in `/health`.
- Deterministic replay base:
  - reads Parquet file or run directory;
  - reconstructs `MarketEvent`;
  - sorts by `ts_local_ms`;
  - feeds the same app pipeline;
  - disables live venue and storage in replay mode.

Verified:

- `cargo fmt`
- `cargo check`
- `cargo test`
- `cargo build -p scalper-app`
- Real Binance smoke for market data and orderbook.
- Capture smoke wrote valid Parquet batch files.
- Replay smoke processed captured events from Parquet.

Next:

1. Record initial orderbook snapshots for full book replay.
2. Latency metrics: event lag, WS jitter, queue depth.
3. Orderflow/CVD/footprint.
4. Heatmap/orderbook feature snapshots.
5. TPO/VP/VWAP.
6. GEX proxy.
7. Hidden-liquidity proxies.
8. Paper broker and risk engine.
9. Execution adapters.
10. MEXC adapter after Binance path stabilizes.
