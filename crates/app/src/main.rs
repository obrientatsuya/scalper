use axum::{Json, Router, extract::State, routing::get};
use clap::Parser;
use scalper_core::{
    adapters::binance::spawn_binance_market_data,
    config::{AppConfig, RunMode},
    events::{Exchange, MarketEvent},
    health::{HealthHandle, HealthSnapshot},
    orderbook::{OrderBookStats, spawn_orderbook_engine},
    shutdown::Shutdown,
    state::RuntimeState,
    storage::{StorageConfig, StorageStats, spawn_parquet_replay, spawn_parquet_storage},
    supervisor::Supervisor,
};
use std::{net::SocketAddr, path::PathBuf, time::Duration};
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let cli = Cli::parse();
    let config = AppConfig::load(&cli.config)?;
    let health = HealthHandle::new(RuntimeState::Booting);
    let shutdown = Shutdown::new();

    let (raw_market_tx, raw_market_rx) = mpsc::channel(config.channels.market_events_capacity);
    let (supervisor_market_tx, market_rx) = mpsc::channel(config.channels.market_events_capacity);
    let (orderbook_market_tx, orderbook_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (storage_market_tx, storage_market_rx) =
        mpsc::channel(config.channels.market_events_capacity);
    let (orderbook_stats_tx, orderbook_stats_rx) = watch::channel(OrderBookStats::default());
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
        orderbook_market_tx,
        storage_market_tx,
        shutdown.child_token(),
    );
    if config.app.mode != RunMode::Replay {
        spawn_orderbook_engine(
            config.app.symbol.clone(),
            orderbook_market_rx,
            orderbook_stats_tx,
            shutdown.child_token(),
        );
    } else {
        drop(orderbook_market_rx);
        drop(orderbook_stats_tx);
    }
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
    orderbook_market_tx: mpsc::Sender<MarketEvent>,
    storage_market_tx: mpsc::Sender<MarketEvent>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                event = raw_market_rx.recv() => {
                    let Some(event) = event else {
                        return;
                    };

                    if supervisor_market_tx.send(event.clone()).await.is_err() {
                        return;
                    }

                    if matches!(event, MarketEvent::DepthDelta(_)) {
                        let _ = orderbook_market_tx.send(event.clone()).await;
                    }

                    let _ = storage_market_tx.send(event).await;
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

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}
