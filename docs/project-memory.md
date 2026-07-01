# Project Memory

Short repo-local handoff only. Full legacy detail lives in
`docs/project-memory.legacy.md`; cross-session memory lives in `claude-mem`.

## Rules

- Commit only when user explicitly asks.
- Push only when user asks.
- No live trading before replay, walk-forward, ablation, paper, and positive
  expectancy after costs.
- Keep active docs short; archive detail instead of duplicating it.

## Current State

- Rust workspace with `scalper-core` and `scalper-app`.
- Binance/MEXC market data, Binance options proxy, Parquet storage/replay,
  orderbook, orderflow, profile/VWAP, GEX, hidden-liquidity, paper broker,
  execution shell, historical imports, regime scan, and research runner exist.
- Active roadmap/status: `docs/implementation-roadmap.md`.
- Full system spec and sector definitions:
  `docs/scalping-amt-gex-orderflow-spec.md#15-roadmap`.

## Current Research Read

- Early smoke result `0.0873R` was ticker-fallback and fragile.
- Trade-enabled aggregate OOS result `0.0633R` is stricter and more useful.
- Latest larger reports show positive OOS expectancy but still weak regime
  coverage.
- Next research goal: fewer trades, more aggressive-flow confirmation, stronger
  OOS validation by regime.
- Research performance improved by deferring orderflow/profile/rolling feature
  stats to emitted snapshots and caching `ProfileTracker` totals/POCs/variance;
  5m high-frequency aggregate smoke now completes in about 2.4s locally.
- Research report includes aggression metrics. Default threshold search now uses
  aggressive grid `[4.0, 5.0]` and picks a threshold that satisfies
  `min-paper-trades` when possible.
- Current 5m aggressive smoke: 119,643 trades -> 85 candidates -> 9 selected
  at threshold 3.0; in-sample looks strong, but OOS remains unproven due small
  selected sample.
- Added grouped chunked aggregate-trade import via `--chunk-ms`; chunks are
  stored under one parent run and research reads the parent recursively.
- Added `import-regime-aggtrades` to import aggTrade windows directly from a
  `regime-scan` JSON report by trend/volatility filters. It supports
  `--skip-windows` so follow-up imports do not duplicate prior matches.
- High-vol aggregate aggressive report:
  `data/reports/research-aggtrades-high-vol-aggregate-aggressive.json`.
  It processed 718,512 aggTrade events, found 347 candidates and 47 selected
  candidates, then reported 13 OOS trades at 3.2178R expectancy across 3
  evaluated splits with 66.7% positive split rate. Treat as promising, not
  complete, because regime coverage remains narrow.
- High-frequency regime classification now uses 1-minute bucket closes plus
  30-minute context around walk-forward test splits, so aggTrade reports do not
  get mislabeled as low-volatility from tiny per-trade returns.
- Research now splits historical streams on gaps over 5 minutes; orderflow,
  profile, heatmap, rolling features, labels, and market-regime coverage no
  longer bridge separate imported windows.
- Added longer 30m downtrend/normal, range/normal, and uptrend/normal aggTrade
  imports through skip-window batches (`w30-2`, `w30-2b`, `w30-2c`).
- Short breakout candidates now require selling book imbalance
  (`obi_20 <= -0.35`). AggTrade-only short breakouts were negative OOS, while
  long breakouts carried the current edge.
- Expanded aggressive default-gate report:
  `data/reports/research-aggtrades-regime-expanded-aggressive-default-gates.json`.
  It processes 2,529,534 aggTrade events with full trend/volatility coverage,
  1,944 candidates, threshold 4.0, 108 paper trades at 0.7878R, 61 OOS trades
  at 1.1389R, 64.7% positive evaluated splits, and
  `data_quality.sufficient = true`.
- Research snapshots now carry GEX regime, total 1% GEX USD, and gamma-flip
  distance when option greeks are present. GEX variants adjust candidate scores
  from those fields; missing/stale GEX leaves candidates unchanged.
- `import-option-greeks` fetches Binance Options greeks snapshots into Parquet
  and supports `--ts-local-ms` for aligned replay smoke. Smoke
  `option-greeks-btc-smoke` stored 572 BTC option-greek events at
  `1780365659999`; research replay read all 572 as `option_greeks`.
- `combine-parquet` merges multiple Parquet runs into one sorted replay run.
  Controlled GEX smoke `gex-oi-repeated-smoke-up-normal-w30-window0` combines
  98,030 aggTrades with 17,160 repeated BTC option-greek events using open
  interest for expiration `260925`.
- `research-gex-oi-repeated-combined-smoke.json` verifies GEX features are
  non-empty: 93/93 candidates and 13/13 selected candidates carry GEX, avg
  selected GEX is `-2,499,628` 1% USD, and GEX ablation diverges from flow-only
  (27 vs 24 selected; 13 vs 12 OOS trades). Treat as plumbing smoke, not
  historical GEX edge, because the option snapshot is repeated over the window.
- Report includes ablations. `amt_flow`/`amt_flow_gex` are still identical on
  the current aggTrade-heavy dataset because it has no option-greek events: 254
  selected, 173 OOS trades, 0.1145R, 45.5% positive splits. Full
  `amt_flow_gex_heatmap` keeps 108 selected, 61 OOS trades, 1.1389R, 64.7%
  positive splits. Heatmap/book confirmation currently carries real filter
  value; GEX needs historical option greek replay for measurement.

## Next

1. Capture or import real time-aligned option-greek history for GEX ablation.
2. Add cost/slippage/liquidation assumptions before paper broker validation.
3. Profile remaining research cost: Parquet read, candidate scoring, and
   walk-forward threshold scans.

## Deployment Target

- Production target is an old notebook running headless Alpine Linux diskless
  from a bootable USB stick, 24/7.
- Final scalper refactor should prioritize energy efficiency, low RAM pressure,
  security, and stable long-running operation on that low-power server.
- Avoid frequent USB writes. The boot USB should mostly hold OS overlay,
  binaries, config, and emergency local state. Runtime heartbeat/status should
  live in RAM when local, and durable data should move to cloud storage/database.
- Durable scalper data should live in Supabase/cloud instead of relying on local
  USB writes. Local Parquet capture on the USB-backed ext4 image is temporary
  until cloud storage is implemented.
- Observability target:
  - Server writes compact heartbeat/status every 2 minutes.
  - Heartbeat includes server stats: online timestamp, uptime, load, RAM, disk,
    CPU temperature, service status.
  - Heartbeat includes scalper stats: health state, market events, queue depth,
    latency, orderbook sync, storage errors, paper/live PnL, open positions,
    kill switch, and other critical runtime fields.
  - Supabase stores current status with upsert semantics, not unbounded
    heartbeat history.
  - Supabase observes heartbeat age/state changes and calls Telegram API only
    on meaningful transitions: server down, server recovered, scalper down,
    scalper recovered, PnL/risk alert, disk/RAM/temp critical, no market events,
    storage errors, or kill switch.
  - Do not send periodic Telegram spam. Telegram is for state changes and
    critical alerts; status dashboard reads Supabase.
- During normal feature development, do not over-optimize for this deployment
  target yet. Preserve the goal and do the final efficiency/storage refactor
  near production hardening.
