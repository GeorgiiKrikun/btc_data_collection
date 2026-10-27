use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info};

mod book;
mod config;
mod kraken_futures;
mod kraken_spot;
mod models;
mod writer;

use models::{Market, Record};

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "quant_2=info".into()),
        )
        .init();

    let cfg = config::Config::from_env();
    tokio::fs::create_dir_all("data").await?;
    info!(
        markets = ?cfg.markets,
        spot = ?cfg.spot.iter().map(|i| &i.wire).collect::<Vec<_>>(),
        futures = ?cfg.futures.iter().map(|i| &i.wire).collect::<Vec<_>>(),
        "Collector starting — output in ./data/kraken/"
    );

    let (tx, rx) = mpsc::channel::<Record>(65_536);

    let mut collectors = Vec::new();
    for market in &cfg.markets {
        let tx = tx.clone();
        let handle = match market {
            Market::Spot => {
                let instruments: Arc<[_]> = cfg.spot.clone().into();
                spawn_connection(*market, move || kraken_spot::run(instruments.clone(), tx.clone()))
            }
            Market::Futures => {
                let instruments: Arc<[_]> = cfg.futures.clone().into();
                spawn_connection(*market, move || kraken_futures::run(instruments.clone(), tx.clone()))
            }
        };
        collectors.push(handle);
    }
    // Only the collector tasks hold senders now, so aborting them ends the writer.
    drop(tx);

    let writer_handle = tokio::spawn(writer::writer_task(rx, cfg.markets.clone()));

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
    info!("Shutting down — flushing remaining data...");

    for handle in &collectors {
        handle.abort();
    }
    let _ = writer_handle.await;

    Ok(())
}

/// A session that lasted at least this long counts as healthy, so its failure
/// is treated as a routine drop rather than part of a failure streak.
const HEALTHY_SESSION: Duration = Duration::from_secs(60);
/// Consecutive failures before a disconnect is logged at ERROR (and alerted on).
const ALERT_AFTER_FAILURES: u32 = 3;
const MAX_BACKOFF: Duration = Duration::from_secs(30);

/// Runs one exchange connection forever. A clean close reconnects
/// immediately. After an error the delay starts at 1s and doubles on each
/// consecutive failure (up to 30s). Exchanges drop long-lived connections now
/// and then, so a drop after a healthy session is logged at INFO. Only
/// repeated failures are logged at ERROR, which logwatch forwards to Telegram.
/// Longer outages are also caught by logwatch's frozen-counter alerts.
fn spawn_connection<F, Fut>(market: Market, mut connect: F) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    tokio::spawn(async move {
        let mut failures: u32 = 0;
        loop {
            let started = Instant::now();
            let result = connect().await;
            let uptime = started.elapsed();
            if uptime >= HEALTHY_SESSION {
                failures = 0;
            }
            match result {
                Ok(()) => info!(market = market.as_str(), "WebSocket closed cleanly, reconnecting immediately..."),
                Err(e) => {
                    failures += 1;
                    let delay = Duration::from_secs(1 << (failures - 1).min(5)).min(MAX_BACKOFF);
                    let up = uptime.as_secs();
                    if failures >= ALERT_AFTER_FAILURES {
                        error!(market = market.as_str(), failures, "WebSocket error: {e:#} ({failures} consecutive failures), reconnecting in {}s", delay.as_secs());
                    } else {
                        info!(market = market.as_str(), uptime_secs = up, "WebSocket dropped after {up}s: {e:#}, reconnecting in {}s", delay.as_secs());
                    }
                    sleep(delay).await;
                }
            }
        }
    })
}
