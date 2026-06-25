use crate::models::{DepthTick, TradeTick};
use chrono::Utc;
use std::path::Path;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{error, info};

const FLUSH_INTERVAL_SECS: u64 = 5;
const FLUSH_THRESHOLD: usize = 500;

pub async fn writer_task(
    mut depth_rx: mpsc::Receiver<DepthTick>,
    mut trade_rx: mpsc::Receiver<TradeTick>,
) {
    let mut depth_buf: Vec<DepthTick> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut trade_buf: Vec<TradeTick> = Vec::with_capacity(FLUSH_THRESHOLD);
    let mut flush_tick = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_tick.tick().await; // consume the immediate first tick
    let mut depth_closed = false;
    let mut trade_closed = false;
    let mut total_depth: u64 = 0;
    let mut total_trades: u64 = 0;

    loop {
        if depth_closed && trade_closed {
            break;
        }

        tokio::select! {
            biased;
            _ = flush_tick.tick() => {
                info!("Flush tick: depth_buf={}, trade_buf={}, total depth={total_depth}, total trades={total_trades}", depth_buf.len(), trade_buf.len());
                if !depth_buf.is_empty() { flush_depth(&mut depth_buf).await; }
                if !trade_buf.is_empty() { flush_trades(&mut trade_buf).await; }
            }
            msg = depth_rx.recv(), if !depth_closed => match msg {
                Some(tick) => {
                    total_depth += 1;
                    depth_buf.push(tick);
                    if depth_buf.len() >= FLUSH_THRESHOLD {
                        flush_depth(&mut depth_buf).await;
                    }
                }
                None => { depth_closed = true; }
            },
            msg = trade_rx.recv(), if !trade_closed => match msg {
                Some(tick) => {
                    total_trades += 1;
                    trade_buf.push(tick);
                    if trade_buf.len() >= FLUSH_THRESHOLD {
                        flush_trades(&mut trade_buf).await;
                    }
                }
                None => { trade_closed = true; }
            },
        }
    }

    if !depth_buf.is_empty() { flush_depth(&mut depth_buf).await; }
    if !trade_buf.is_empty() { flush_trades(&mut trade_buf).await; }
    info!("Writer flushed and exiting");
}

async fn flush_depth(buf: &mut Vec<DepthTick>) {
    let records = std::mem::take(buf);
    let n = records.len();
    let date = Utc::now().format("%Y%m%d").to_string();
    let path = format!("data/depth5_tick_{date}.csv");
    match write_csv_records(&path, records).await {
        Ok(()) => info!("Wrote {n} depth rows → {path}"),
        Err(e) => error!("depth flush error: {e}"),
    }
}

async fn flush_trades(buf: &mut Vec<TradeTick>) {
    let records = std::mem::take(buf);
    let n = records.len();
    let date = Utc::now().format("%Y%m%d").to_string();
    let path = format!("data/trades_tick_{date}.csv");
    match write_csv_records(&path, records).await {
        Ok(()) => info!("Wrote {n} trade rows → {path}"),
        Err(e) => error!("trades flush error: {e}"),
    }
}

async fn write_csv_records<T: serde::Serialize + Send + 'static>(
    path: &str,
    records: Vec<T>,
) -> anyhow::Result<()> {
    let needs_header = !Path::new(path).exists();
    let path = path.to_string();

    tokio::task::spawn_blocking(move || {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;

        let mut writer = csv::WriterBuilder::new()
            .has_headers(needs_header)
            .from_writer(file);

        for record in records {
            writer.serialize(record)?;
        }
        writer.flush()?;
        Ok::<_, anyhow::Error>(())
    })
    .await??;

    Ok(())
}
