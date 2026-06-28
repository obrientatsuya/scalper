use axum::{Json, Router, extract::State, routing::get};
use clap::{Parser, Subcommand};
use scalper_core::{
    adapters::{binance::spawn_binance_market_data, mexc::spawn_mexc_market_data},
    config::{AppConfig, RunMode},
    events::{Exchange, MarketEvent},
    execution::BinanceDemoExecutionAdapter,
    gex::{GexStats, spawn_gex_engine},
    health::{HealthHandle, HealthSnapshot},
    heatmap::{HeatmapStats, spawn_heatmap_engine},
    hidden::{HiddenLiquidityStats, spawn_hidden_engine},
    historical::{
        BinanceAggTradeImportConfig, BinanceKlineImportConfig, default_agg_trades_run_name,
        default_kline_run_name, fetch_binance_agg_trades, fetch_binance_klines,
    },
    latency::{LatencyTracker, QueueDepthStats},
    orderbook::{OrderBookStats, spawn_orderbook_engine},
    orderflow::{OrderflowStats, spawn_orderflow_engine},
    paper::PaperBroker,
    profile::{ProfileStats, spawn_profile_engine},
    research_runner::{ResearchRunConfig, run_research_from_parquet, scan_regimes_from_parquet},
    shutdown::Shutdown,
    state::RuntimeState,
    storage::{
        StorageConfig, StorageStats, spawn_parquet_replay, spawn_parquet_storage,
        write_events_to_parquet_run,
    },
    supervisor::Supervisor,
};
use std::{fs, net::SocketAddr, path::PathBuf, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    time::MissedTickBehavior,
};
use tracing::{info, warn};
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Debug, Parser)]
struct Cli {
    #[arg(short, long, default_value = "config/scalper.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Research {
        #[arg(short, long)]
        input: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(long, default_value_t = 500)]
        train_size: usize,
        #[arg(long, default_value_t = 100)]
        test_size: usize,
        #[arg(long, default_value_t = 0)]
        min_snapshot_interval_ms: i64,
        #[arg(long, default_value_t = 500)]
        min_trade_events: usize,
        #[arg(long, default_value_t = 100)]
        min_paper_trades: usize,
        #[arg(long, default_value_t = 1)]
        min_walk_forward_splits: usize,
        #[arg(long, default_value_t = 50)]
        min_oos_trades: usize,
        #[arg(long, default_value_t = 0.0)]
        min_oos_expectancy_r: f64,
        #[arg(long, default_value_t = 0.60)]
        min_positive_oos_split_rate: f64,
        #[arg(long, default_value_t = 3)]
        min_trend_regimes: usize,
        #[arg(long, default_value_t = 3)]
        min_volatility_regimes: usize,
        #[arg(long, default_value_t = false)]
        omit_labels: bool,
    },
    ImportKlines {
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long, default_value = "1m")]
        interval: String,
        #[arg(long)]
        start_time_ms: Option<i64>,
        #[arg(long)]
        end_time_ms: Option<i64>,
        #[arg(long, default_value_t = 168)]
        lookback_hours: i64,
        #[arg(long, default_value = "data/historical/binance-klines")]
        output_root: PathBuf,
        #[arg(long)]
        run_name: Option<String>,
        #[arg(long, default_value_t = 1000)]
        flush_batch_size: usize,
    },
    ImportAggtrades {
        #[arg(long, default_value = "BTCUSDT")]
        symbol: String,
        #[arg(long)]
        start_time_ms: i64,
        #[arg(long)]
        end_time_ms: i64,
        #[arg(long, default_value = "data/historical/binance-aggtrades")]
        output_root: PathBuf,
        #[arg(long)]
        run_name: Option<String>,
        #[arg(long, default_value_t = 1000)]
        flush_batch_size: usize,
    },
    RegimeScan {
        #[arg(short, long)]
        input: PathBuf,
        #[arg(short, long)]
        output: Option<PathBuf>,
        #[arg(long, default_value_t = 240)]
        window_size: usize,
        #[arg(long, default_value_t = 60)]
        step_size: usize,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    match cli.command {
        Some(Command::Research {
            input,
            output,
            train_size,
            test_size,
            min_snapshot_interval_ms,
            min_trade_events,
            min_paper_trades,
            min_walk_forward_splits,
            min_oos_trades,
            min_oos_expectancy_r,
            min_positive_oos_split_rate,
            min_trend_regimes,
            min_volatility_regimes,
            omit_labels,
        }) => {
            return run_research_command(
                input,
                output,
                train_size,
                test_size,
                min_snapshot_interval_ms,
                min_trade_events,
                min_paper_trades,
                min_walk_forward_splits,
                min_oos_trades,
                min_oos_expectancy_r,
                min_positive_oos_split_rate,
                min_trend_regimes,
                min_volatility_regimes,
                omit_labels,
            );
        }
        Some(Command::ImportKlines {
            symbol,
            interval,
            start_time_ms,
            end_time_ms,
            lookback_hours,
            output_root,
            run_name,
            flush_batch_size,
        }) => {
            return run_import_klines_command(
                symbol,
                interval,
                start_time_ms,
                end_time_ms,
                lookback_hours,
                output_root,
                run_name,
                flush_batch_size,
            )
            .await;
        }
        Some(Command::ImportAggtrades {
            symbol,
            start_time_ms,
            end_time_ms,
            output_root,
            run_name,
            flush_batch_size,
        }) => {
            return run_import_aggtrades_command(
                symbol,
                start_time_ms,
                end_time_ms,
                output_root,
                run_name,
                flush_batch_size,
            )
            .await;
        }
        Some(Command::RegimeScan {
            input,
            output,
            window_size,
            step_size,
        }) => {
            return run_regime_scan_command(input, output, window_size, step_size);
        }
        None => {}
    }

    let config = AppConfig::load(&cli.config)?;
    let health = HealthHandle::new(RuntimeState::Booting);
    let shutdown = Shutdown::new();
    health
        .set_paper_stats(PaperBroker::new(config.risk.clone(), 5.0).stats())
        .await;
    health
        .set_execution_stats(BinanceDemoExecutionAdapter::new().stats())
        .await;

    let (raw_market_tx, raw_market_rx) = mpsc::channel(config.channels.market_events_capacity);
    let (supervisor_market_tx, market_rx) = mpsc::channel(config.channels.market_events_capacity);
    let (orderbook_market_tx, orderbook_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (orderflow_market_tx, orderflow_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (profile_market_tx, profile_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (gex_market_tx, gex_market_rx) = mpsc::channel(config.channels.market_events_capacity);
    let (hidden_market_tx, hidden_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (heatmap_market_tx, heatmap_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (storage_market_tx, storage_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (orderbook_stats_tx, orderbook_stats_rx) = watch::channel(OrderBookStats::default());
    let (orderflow_stats_tx, orderflow_stats_rx) = watch::channel(OrderflowStats::default());
    let (profile_stats_tx, profile_stats_rx) = watch::channel(ProfileStats::default());
    let (gex_stats_tx, gex_stats_rx) = watch::channel(GexStats::default());
    let (hidden_stats_tx, hidden_stats_rx) = watch::channel(HiddenLiquidityStats::default());
    let (heatmap_stats_tx, heatmap_stats_rx) = watch::channel(HeatmapStats::default());
    let (storage_stats_tx, storage_stats_rx) = watch::channel(StorageStats::default());

    let real_market_data_started = spawn_market_source(&config, raw_market_tx.clone(), &shutdown)?;

    spawn_ctrl_c_handler(shutdown.clone());
    if !real_market_data_started {
        warn!("no real market data venue enabled; using stub heartbeat source");
        spawn_heartbeat_source(raw_market_tx, shutdown.child_token());
    }
    spawn_market_fanout(
        raw_market_rx,
        supervisor_market_tx,
        orderflow_market_tx,
        profile_market_tx,
        gex_market_tx,
        hidden_market_tx,
        heatmap_market_tx,
        orderbook_market_tx,
        storage_market_tx.clone(),
        health.clone(),
        shutdown.child_token(),
    );
    spawn_orderflow_engine(
        orderflow_market_rx,
        orderflow_stats_tx,
        shutdown.child_token(),
    );
    spawn_profile_engine(profile_market_rx, profile_stats_tx, shutdown.child_token());
    spawn_gex_engine(gex_market_rx, gex_stats_tx, shutdown.child_token());
    spawn_hidden_engine(hidden_market_rx, hidden_stats_tx, shutdown.child_token());
    spawn_heatmap_engine(heatmap_market_rx, heatmap_stats_tx, shutdown.child_token());
    spawn_orderbook_engine(
        config.app.symbol.clone(),
        orderbook_market_rx,
        orderbook_stats_tx,
        (config.app.mode != RunMode::Replay).then_some(storage_market_tx.clone()),
        config.app.mode != RunMode::Replay,
        shutdown.child_token(),
    );
    spawn_parquet_storage(
        StorageConfig {
            enabled: config.storage.enabled && config.app.mode != RunMode::Replay,
            root_dir: config.storage.root_dir.clone().into(),
            symbol: config.app.symbol.clone(),
            flush_batch_size: config.storage.flush_batch_size,
            flush_interval: config.storage.flush_interval,
        },
        storage_market_rx,
        storage_stats_tx,
        shutdown.child_token(),
    );
    spawn_heatmap_health_bridge(health.clone(), heatmap_stats_rx, shutdown.child_token());
    spawn_orderflow_health_bridge(health.clone(), orderflow_stats_rx, shutdown.child_token());
    spawn_profile_health_bridge(health.clone(), profile_stats_rx, shutdown.child_token());
    spawn_gex_health_bridge(health.clone(), gex_stats_rx, shutdown.child_token());
    spawn_hidden_health_bridge(health.clone(), hidden_stats_rx, shutdown.child_token());
    spawn_orderbook_health_bridge(health.clone(), orderbook_stats_rx, shutdown.child_token());
    spawn_storage_health_bridge(health.clone(), storage_stats_rx, shutdown.child_token());
    spawn_health_server(
        config.health.bind.parse()?,
        health.clone(),
        shutdown.child_token(),
    );

    Supervisor::new(config, health, market_rx, shutdown.child_token())
        .run()
        .await?;

    Ok(())
}

async fn run_import_aggtrades_command(
    symbol: String,
    start_time_ms: i64,
    end_time_ms: i64,
    output_root: PathBuf,
    run_name: Option<String>,
    flush_batch_size: usize,
) -> anyhow::Result<()> {
    let events = fetch_binance_agg_trades(BinanceAggTradeImportConfig {
        symbol: symbol.clone(),
        start_time_ms,
        end_time_ms,
    })
    .await?;
    let run_name = run_name
        .unwrap_or_else(|| default_agg_trades_run_name(&symbol, start_time_ms, end_time_ms));
    let events_written = events.len();
    let run_dir =
        write_events_to_parquet_run(output_root, &symbol, &run_name, events, flush_batch_size)?;

    println!(
        "{}",
        serde_json::json!({
            "symbol": symbol,
            "start_time_ms": start_time_ms,
            "end_time_ms": end_time_ms,
            "events_written": events_written,
            "run_dir": run_dir,
        })
    );

    Ok(())
}

async fn run_import_klines_command(
    symbol: String,
    interval: String,
    start_time_ms: Option<i64>,
    end_time_ms: Option<i64>,
    lookback_hours: i64,
    output_root: PathBuf,
    run_name: Option<String>,
    flush_batch_size: usize,
) -> anyhow::Result<()> {
    let end = end_time_ms.unwrap_or_else(now_ms);
    let start = start_time_ms.unwrap_or(end - lookback_hours * 60 * 60 * 1000);
    let events = fetch_binance_klines(BinanceKlineImportConfig {
        symbol: symbol.clone(),
        interval: interval.clone(),
        start_time_ms: start,
        end_time_ms: end,
    })
    .await?;
    let run_name =
        run_name.unwrap_or_else(|| default_kline_run_name(&symbol, &interval, start, end));
    let events_written = events.len();
    let run_dir =
        write_events_to_parquet_run(output_root, &symbol, &run_name, events, flush_batch_size)?;

    println!(
        "{}",
        serde_json::json!({
            "symbol": symbol,
            "interval": interval,
            "start_time_ms": start,
            "end_time_ms": end,
            "events_written": events_written,
            "run_dir": run_dir,
        })
    );

    Ok(())
}

fn run_research_command(
    input: PathBuf,
    output: Option<PathBuf>,
    train_size: usize,
    test_size: usize,
    min_snapshot_interval_ms: i64,
    min_trade_events: usize,
    min_paper_trades: usize,
    min_walk_forward_splits: usize,
    min_oos_trades: usize,
    min_oos_expectancy_r: f64,
    min_positive_oos_split_rate: f64,
    min_trend_regimes: usize,
    min_volatility_regimes: usize,
    omit_labels: bool,
) -> anyhow::Result<()> {
    let mut report = run_research_from_parquet(
        input,
        ResearchRunConfig {
            train_size,
            test_size,
            min_snapshot_interval_ms,
            min_trade_events,
            min_paper_trades,
            min_walk_forward_splits,
            min_oos_trades,
            min_oos_expectancy_r,
            min_positive_oos_split_rate,
            min_trend_regimes,
            min_volatility_regimes,
            ..Default::default()
        },
    )?;
    if omit_labels {
        report.labels.clear();
    }
    let json = serde_json::to_string_pretty(&report)?;

    if let Some(output) = output {
        fs::write(output, json)?;
    } else {
        println!("{json}");
    }

    Ok(())
}

fn spawn_market_source(
    config: &AppConfig,
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: &Shutdown,
) -> anyhow::Result<bool> {
    if config.app.mode == RunMode::Replay {
        let replay = config
            .replay
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("replay mode requires [replay].input_path"))?;
        spawn_parquet_replay(
            replay.input_path.clone().into(),
            market_tx,
            shutdown.child_token(),
        );
        return Ok(true);
    }

    let mut started = false;

    for venue in &config.venues {
        if !(venue.enabled && venue.market_data) {
            continue;
        }

        match venue.exchange {
            Exchange::Binance => {
                spawn_binance_market_data(
                    config.app.symbol.clone(),
                    market_tx.clone(),
                    shutdown.child_token(),
                );
                started = true;
            }
            Exchange::Mexc => {
                spawn_mexc_market_data(
                    config.app.symbol.clone(),
                    market_tx.clone(),
                    shutdown.child_token(),
                );
                started = true;
            }
            exchange => {
                warn!(?exchange, "market data adapter not implemented yet");
            }
        }
    }

    Ok(started)
}

fn spawn_market_fanout(
    mut raw_market_rx: mpsc::Receiver<MarketEvent>,
    supervisor_market_tx: mpsc::Sender<MarketEvent>,
    orderflow_market_tx: mpsc::Sender<MarketEvent>,
    profile_market_tx: mpsc::Sender<MarketEvent>,
    gex_market_tx: mpsc::Sender<MarketEvent>,
    hidden_market_tx: mpsc::Sender<MarketEvent>,
    heatmap_market_tx: mpsc::Sender<MarketEvent>,
    orderbook_market_tx: mpsc::Sender<MarketEvent>,
    storage_market_tx: mpsc::Sender<MarketEvent>,
    health: HealthHandle,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut latency = LatencyTracker::default();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = raw_market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    let latency_stats = latency.observe(
                        &event,
                        QueueDepthStats {
                            raw_market_rx: raw_market_rx.len(),
                            supervisor_tx: channel_depth(&supervisor_market_tx),
                            orderflow_tx: channel_depth(&orderflow_market_tx),
                            profile_tx: channel_depth(&profile_market_tx),
                            gex_tx: channel_depth(&gex_market_tx),
                            hidden_tx: channel_depth(&hidden_market_tx),
                            heatmap_tx: channel_depth(&heatmap_market_tx),
                            orderbook_tx: channel_depth(&orderbook_market_tx),
                            storage_tx: channel_depth(&storage_market_tx),
                        },
                    );
                    health.set_latency_stats(latency_stats).await;

                    if supervisor_market_tx.send(event.clone()).await.is_err() {
                        return;
                    }

                    if matches!(event, MarketEvent::Trade(_)) {
                        let _ = orderflow_market_tx.send(event.clone()).await;
                        let _ = profile_market_tx.send(event.clone()).await;
                    }

                    if matches!(
                        event,
                        MarketEvent::Trade(_) | MarketEvent::Ticker(_) | MarketEvent::OptionGreek(_)
                    ) {
                        let _ = gex_market_tx.send(event.clone()).await;
                    }

                    if matches!(
                        event,
                        MarketEvent::Trade(_) | MarketEvent::Ticker(_) | MarketEvent::DepthDelta(_)
                    ) {
                        let _ = hidden_market_tx.send(event.clone()).await;
                    }

                    if matches!(
                        event,
                        MarketEvent::DepthDelta(_) | MarketEvent::OrderBookSnapshot(_)
                    ) {
                        let _ = heatmap_market_tx.send(event.clone()).await;
                        let _ = orderbook_market_tx.send(event.clone()).await;
                    }

                    let _ = storage_market_tx.send(event).await;
                }
            }
        }
    });
}

fn channel_depth<T>(tx: &mpsc::Sender<T>) -> usize {
    tx.max_capacity().saturating_sub(tx.capacity())
}

fn spawn_heatmap_health_bridge(
    health: HealthHandle,
    mut heatmap_stats_rx: watch::Receiver<HeatmapStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = heatmap_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = heatmap_stats_rx.borrow().clone();
                    health.set_heatmap_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_orderflow_health_bridge(
    health: HealthHandle,
    mut orderflow_stats_rx: watch::Receiver<OrderflowStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = orderflow_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = orderflow_stats_rx.borrow().clone();
                    health.set_orderflow_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_profile_health_bridge(
    health: HealthHandle,
    mut profile_stats_rx: watch::Receiver<ProfileStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = profile_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = profile_stats_rx.borrow().clone();
                    health.set_profile_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_gex_health_bridge(
    health: HealthHandle,
    mut gex_stats_rx: watch::Receiver<GexStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = gex_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = gex_stats_rx.borrow().clone();
                    health.set_gex_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_hidden_health_bridge(
    health: HealthHandle,
    mut hidden_stats_rx: watch::Receiver<HiddenLiquidityStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = hidden_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = hidden_stats_rx.borrow().clone();
                    health.set_hidden_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_orderbook_health_bridge(
    health: HealthHandle,
    mut orderbook_stats_rx: watch::Receiver<OrderBookStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = orderbook_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = orderbook_stats_rx.borrow().clone();
                    health.set_orderbook_stats(stats).await;
                }
            }
        }
    });
}

fn spawn_storage_health_bridge(
    health: HealthHandle,
    mut storage_stats_rx: watch::Receiver<StorageStats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                changed = storage_stats_rx.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let stats = storage_stats_rx.borrow().clone();
                    health.set_storage_stats(stats).await;
                }
            }
        }
    });
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).json().init();
}

fn spawn_ctrl_c_handler(shutdown: Shutdown) {
    tokio::spawn(async move {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to listen for ctrl-c");
        }
        shutdown.cancel();
    });
}

fn spawn_heartbeat_source(
    market_tx: mpsc::Sender<MarketEvent>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(Duration::from_secs(1));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticks.tick() => {
                    let event = MarketEvent::Heartbeat {
                        component: "stub_market_data".to_string(),
                        ts_local_ms: now_ms(),
                    };
                    if market_tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
}

fn spawn_health_server(
    bind: SocketAddr,
    health: HealthHandle,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        let app = Router::new()
            .route("/health", get(health_route))
            .with_state(health);

        let listener = match TcpListener::bind(bind).await {
            Ok(listener) => listener,
            Err(error) => {
                warn!(%error, %bind, "failed to bind health server");
                return;
            }
        };

        info!(%bind, "health server listening");
        let server = axum::serve(listener, app).with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        });

        if let Err(error) = server.await {
            warn!(%error, "health server failed");
        }
    });
}

async fn health_route(State(health): State<HealthHandle>) -> Json<HealthSnapshot> {
    Json(health.snapshot().await)
}

fn run_regime_scan_command(
    input: PathBuf,
    output: Option<PathBuf>,
    window_size: usize,
    step_size: usize,
) -> anyhow::Result<()> {
    let report = scan_regimes_from_parquet(input, window_size, step_size)?;
    let json = serde_json::to_string_pretty(&report)?;

    if let Some(output) = output {
        fs::write(output, json)?;
    } else {
        println!("{json}");
    }

    Ok(())
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
