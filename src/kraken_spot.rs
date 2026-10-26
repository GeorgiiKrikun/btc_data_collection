//! Kraken spot WebSocket v2: top-10 book (sampled every 100ms) and trades.

use crate::book::{Book, Desync};
use crate::config::Instrument;
use crate::models::{
    Market, Record, Row, SpotBookData, SpotHead, SpotMsg, SpotTradeData, SpotTradeTick, DEPTH,
};
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const WS_URL: &str = "wss://ws.kraken.com/v2";
const SAMPLE_EVERY: Duration = Duration::from_millis(100);
/// Kraken sends a heartbeat about every second, so silence means a dead link.
const MAX_SILENCE: Duration = Duration::from_secs(10);

struct Tracked {
    instrument: Arc<str>,
    book: Book,
}

pub async fn run(instruments: Arc<[Instrument]>, tx: mpsc::Sender<Record>) -> Result<()> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!(market = "spot", "Connected to Kraken spot WebSocket");
    let (mut write, mut read) = ws_stream.split();

    let symbols: Vec<&str> = instruments.iter().map(|i| i.wire.as_str()).collect();
    let subs = [
        json!({"method": "subscribe", "params": {"channel": "book", "symbol": symbols, "depth": DEPTH}}),
        json!({"method": "subscribe", "params": {"channel": "trade", "symbol": symbols, "snapshot": false}}),
    ];
    for sub in subs {
        write.send(Message::Text(sub.to_string())).await?;
    }

    let mut books: HashMap<String, Tracked> = instruments
        .iter()
        .map(|i| {
            let tracked = Tracked { instrument: i.dir.clone(), book: Book::new(Some(DEPTH)) };
            (i.wire.clone(), tracked)
        })
        .collect();

    let mut sample = interval(SAMPLE_EVERY);
    sample.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_msg = Instant::now();

    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else { break };
                last_msg = Instant::now();
                let recv_ts_us = Utc::now().timestamp_micros();
                match msg? {
                    Message::Text(text) => {
                        if let Err(e) = handle_message(&text, recv_ts_us, &mut books, &tx).await {
                            if e.is::<Desync>() {
                                return Err(e);
                            }
                            warn!(market = "spot", "Message error: {e:#}");
                        }
                    }
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = sample.tick() => {
                if last_msg.elapsed() > MAX_SILENCE {
                    bail!("no message from Kraken spot for {}s", last_msg.elapsed().as_secs());
                }
                let recv_ts_us = Utc::now().timestamp_micros();
                for t in books.values_mut() {
                    if let Some(tick) = t.book.take_sample(recv_ts_us) {
                        let rec = Record { market: Market::Spot, instrument: t.instrument.clone(), row: Row::Depth(tick) };
                        let _ = tx.send(rec).await;
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_message(
    text: &str,
    recv_ts_us: i64,
    books: &mut HashMap<String, Tracked>,
    tx: &mpsc::Sender<Record>,
) -> Result<()> {
    let head: SpotHead = serde_json::from_str(text)?;

    if head.method == Some("subscribe") {
        match head.success {
            Some(true) => debug!(market = "spot", result = ?head.result, "subscribed"),
            _ => error!(market = "spot", "subscribe failed: {}", head.error.unwrap_or_default()),
        }
        return Ok(());
    }

    match head.channel {
        Some("book") => {
            let snapshot = head.kind == Some("snapshot");
            let msg: SpotMsg<SpotBookData> = serde_json::from_str(text)?;
            for d in msg.data {
                let Some(t) = books.get_mut(&d.symbol) else { continue };
                if snapshot {
                    t.book.clear();
                }
                t.book.apply(&d.bids, &d.asks);
                let local = t.book.checksum();
                if local != d.checksum {
                    return Err(Desync(format!(
                        "spot {} checksum {local} != exchange {}",
                        d.symbol, d.checksum
                    ))
                    .into());
                }
                t.book.touch(parse_ts_us(&d.timestamp)?);
            }
        }
        Some("trade") => {
            let msg: SpotMsg<SpotTradeData> = serde_json::from_str(text)?;
            for d in msg.data {
                let Some(t) = books.get(&d.symbol) else { continue };
                let tick = SpotTradeTick {
                    recv_ts_us,
                    exch_ts_us: parse_ts_us(&d.timestamp)?,
                    trade_id: d.trade_id,
                    price: d.price,
                    qty: d.qty,
                    side: d.side,
                    ord_type: d.ord_type,
                };
                let rec = Record { market: Market::Spot, instrument: t.instrument.clone(), row: Row::SpotTrade(tick) };
                let _ = tx.send(rec).await;
            }
        }
        // "heartbeat" keeps `last_msg` fresh; "status" announces maintenance etc.
        Some("status") => debug!(market = "spot", "status: {text}"),
        _ => {}
    }
    Ok(())
}

fn parse_ts_us(ts: &str) -> Result<i64> {
    Ok(DateTime::parse_from_rfc3339(ts)?.timestamp_micros())
}
