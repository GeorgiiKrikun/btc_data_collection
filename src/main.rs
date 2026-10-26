use anyhow::Result;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{sleep, Duration};
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

/// Runs one exchange connection forever: reconnects immediately after a clean
/// close and 5s after an error.
fn spawn_connection<F, Fut>(market: Market, mut connect: F) -> JoinHandle<()>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Result<()>> + Send,
{
    tokio::spawn(async move {
        loop {
            match connect().await {
                Ok(()) => info!(market = market.as_str(), "WebSocket closed cleanly, reconnecting immediately..."),
                Err(e) => {
                    error!(market = market.as_str(), "WebSocket error: {e:#}, reconnecting in 5s");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    })
}
