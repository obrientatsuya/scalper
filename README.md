# scalper

Rust market-data and research pipeline for crypto futures scalping.

## What it does

- Streams Binance and MEXC futures market data.
- Records and replays Parquet market events.
- Builds orderflow, orderbook, profile, GEX, heatmap, and hidden-liquidity signals.
- Runs historical imports, regime scans, and walk-forward research.
- Exposes runtime state through `/health`.

## Commands

```powershell
cargo run -- --config config/scalper.toml
cargo run -- research --input data/parquet/run
cargo run -- import-klines --symbol BTCUSDT --interval 1m
cargo run -- regime-scan --input data/historical/binance-klines/btcusdt/run
```

## Status

Research-first. Paper execution exists; live execution should stay gated.

## License

UNLICENSED.
