use anyhow::Result;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{error, info};

mod collector;
mod models;
mod writer;

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "quant_2=info".into()),
        )
        .init();

    tokio::fs::create_dir_all("data").await?;
    info!("Collector starting — output in ./data/");

    let (depth_tx, depth_rx) = mpsc::channel(8192);
    let (trade_tx, trade_rx) = mpsc::channel(8192);

    let ws_handle = tokio::spawn(async move {
        loop {
            match collector::run(depth_tx.clone(), trade_tx.clone()).await {
                Ok(()) => info!("WebSocket closed cleanly, reconnecting immediately..."),
                Err(e) => {
                    error!("WebSocket error: {e}, reconnecting in 5s");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    });

    let writer_handle = tokio::spawn(writer::writer_task(depth_rx, trade_rx));

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
    info!("Shutting down — flushing remaining data...");

    ws_handle.abort();
    let _ = writer_handle.await;

    Ok(())
}
