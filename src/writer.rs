use crate::models::{Market, Record, Row};
use chrono::Utc;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tracing::{debug, error, info};

const FLUSH_INTERVAL_SECS: u64 = 5;
const FLUSH_THRESHOLD: usize = 500;

/// One output file family: `data/kraken/<market>/<instrument>/<stream>_YYYYMMDD.csv`.
type BufKey = (Market, Arc<str>, &'static str);

#[derive(Default)]
struct Counters {
    depth: u64,
    trades: u64,
    ticker: u64,
}

pub async fn writer_task(mut rx: mpsc::Receiver<Record>, markets: Vec<Market>) {
    let mut bufs: HashMap<BufKey, Vec<Row>> = HashMap::new();
    let mut counters: HashMap<Market, Counters> =
        markets.iter().map(|m| (*m, Counters::default())).collect();
    let mut flush_tick = interval(Duration::from_secs(FLUSH_INTERVAL_SECS));
    flush_tick.tick().await; // consume the immediate first tick

    loop {
        tokio::select! {
            biased;
            _ = flush_tick.tick() => {
                // NOTE: the `total_depth` field, the "flush tick" message and the
                // per-market "market tick" events are a contract consumed by the
                // `logwatch` binary — keep them in sync.
                let total_depth: u64 = counters.values().map(|c| c.depth).sum();
                let total_trades: u64 = counters.values().map(|c| c.trades).sum();
                let buffered: usize = bufs.values().map(Vec::len).sum();
                info!(buffered, total_depth, total_trades, "flush tick");
                for m in &markets {
                    let c = &counters[m];
                    info!(market = m.as_str(), depth = c.depth, trades = c.trades, ticker = c.ticker, "market tick");
                }
                flush_all(&mut bufs).await;
            }
            msg = rx.recv() => match msg {
                Some(rec) => {
                    let c = counters.entry(rec.market).or_default();
                    match rec.row {
                        Row::Depth(_) => c.depth += 1,
                        Row::SpotTrade(_) | Row::FutTrade(_) => c.trades += 1,
                        Row::Ticker(_) => c.ticker += 1,
                    }
                    let key = (rec.market, rec.instrument, rec.row.stream());
                    let buf = bufs.entry(key.clone()).or_default();
                    buf.push(rec.row);
                    if buf.len() >= FLUSH_THRESHOLD {
                        let records = std::mem::take(buf);
                        flush(&key, records).await;
                    }
                }
                None => break,
            },
        }
    }

    flush_all(&mut bufs).await;
    info!("Writer flushed and exiting");
}

async fn flush_all(bufs: &mut HashMap<BufKey, Vec<Row>>) {
    for (key, buf) in bufs.iter_mut() {
        if !buf.is_empty() {
            flush(key, std::mem::take(buf)).await;
        }
    }
}

async fn flush((market, instrument, stream): &BufKey, records: Vec<Row>) {
    let n = records.len();
    let date = Utc::now().format("%Y%m%d").to_string();
    let dir = format!("data/kraken/{}/{instrument}", market.as_str());
    let path = format!("{dir}/{stream}_{date}.csv");
    let result = match tokio::fs::create_dir_all(&dir).await {
        Ok(()) => write_csv_records(&path, records).await,
        Err(e) => Err(e.into()),
    };
    match result {
        Ok(()) => debug!(rows = n, path = %path, "wrote rows"),
        Err(e) => error!("flush error for {path}: {e}"),
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
