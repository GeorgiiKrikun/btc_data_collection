//! Kraken Futures WebSocket v1: full book (sampled to top-10 every 100ms),
//! trades (incl. liquidations) and the 1s ticker (mark/index/funding/OI).

use crate::book::{Book, Desync, Side};
use crate::config::Instrument;
use crate::models::{
    FuturesBookDelta, FuturesBookSnapshot, FuturesHead, FuturesTickerData, FuturesTradeData,
    FuturesTradeTick, Market, Record, Row, TickerTick,
};
use anyhow::{bail, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

const WS_URL: &str = "wss://futures.kraken.com/ws/v1";
const SAMPLE_EVERY: Duration = Duration::from_millis(100);
/// Kraken drops connections that don't ping at least every 60s.
const PING_EVERY: Duration = Duration::from_secs(30);
/// The ticker alone arrives every second per product, so silence means a dead link.
const MAX_SILENCE: Duration = Duration::from_secs(10);

struct Tracked {
    instrument: Arc<str>,
    book: Book,
    /// last applied book sequence number; `None` until the snapshot arrives
    seq: Option<u64>,
}

pub async fn run(instruments: Arc<[Instrument]>, tx: mpsc::Sender<Record>) -> Result<()> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!(market = "futures", "Connected to Kraken Futures WebSocket");
    let (mut write, mut read) = ws_stream.split();

    let products: Vec<&str> = instruments.iter().map(|i| i.wire.as_str()).collect();
    for feed in ["book", "trade", "ticker"] {
        let sub = json!({"event": "subscribe", "feed": feed, "product_ids": products});
        write.send(Message::Text(sub.to_string())).await?;
    }
    let heartbeat = json!({"event": "subscribe", "feed": "heartbeat"});
    write.send(Message::Text(heartbeat.to_string())).await?;

    let mut books: HashMap<String, Tracked> = instruments
        .iter()
        .map(|i| {
            let tracked = Tracked { instrument: i.dir.clone(), book: Book::new(None), seq: None };
            (i.wire.clone(), tracked)
        })
        .collect();

    let mut sample = interval(SAMPLE_EVERY);
    sample.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut ping = interval(PING_EVERY);
    ping.tick().await; // consume the immediate first tick
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
                            warn!(market = "futures", "Message error: {e:#}");
                        }
                    }
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = ping.tick() => write.send(Message::Ping(Vec::new())).await?,
            _ = sample.tick() => {
                if last_msg.elapsed() > MAX_SILENCE {
                    bail!("no message from Kraken Futures for {}s", last_msg.elapsed().as_secs());
                }
                let recv_ts_us = Utc::now().timestamp_micros();
                for t in books.values_mut() {
                    if let Some(tick) = t.book.take_sample(recv_ts_us) {
                        let rec = Record { market: Market::Futures, instrument: t.instrument.clone(), row: Row::Depth(tick) };
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
    let head: FuturesHead = serde_json::from_str(text)?;

    if let Some(event) = head.event {
        match event {
            "subscribed" => debug!(market = "futures", "subscribed: {text}"),
            "error" | "subscribed_failed" => {
                error!(market = "futures", "subscribe failed: {text}")
            }
            _ => debug!(market = "futures", "event: {text}"),
        }
        return Ok(());
    }

    match head.feed {
        Some("book_snapshot") => {
            let s: FuturesBookSnapshot = serde_json::from_str(text)?;
            let Some(t) = books.get_mut(&s.product_id) else { return Ok(()) };
            t.book.clear();
            t.book.apply(&s.bids, &s.asks);
            t.book.touch(s.timestamp * 1000);
            t.seq = Some(s.seq);
        }
        Some("book") => {
            let d: FuturesBookDelta = serde_json::from_str(text)?;
            let Some(t) = books.get_mut(&d.product_id) else { return Ok(()) };
            // Deltas continue the snapshot's sequence one by one; anything else
            // means we missed an update and the book can't be trusted.
            let Some(prev) = t.seq else { return Ok(()) };
            if d.seq != prev + 1 {
                return Err(Desync(format!(
                    "futures {} seq gap: {prev} -> {}",
                    d.product_id, d.seq
                ))
                .into());
            }
            t.seq = Some(d.seq);
            let side = if d.side == "buy" { Side::Bid } else { Side::Ask };
            t.book.set(side, d.price, d.qty);
            t.book.touch(d.timestamp * 1000);
        }
        Some("trade") => {
            let d: FuturesTradeData = serde_json::from_str(text)?;
            let Some(t) = books.get(&d.product_id) else { return Ok(()) };
            let tick = FuturesTradeTick {
                recv_ts_us,
                exch_ts_ms: d.time,
                seq: d.seq,
                uid: d.uid,
                price: d.price,
                qty: d.qty,
                side: d.side,
                trade_type: d.trade_type,
            };
            let rec = Record { market: Market::Futures, instrument: t.instrument.clone(), row: Row::FutTrade(tick) };
            let _ = tx.send(rec).await;
        }
        Some("ticker") => {
            let d: FuturesTickerData = serde_json::from_str(text)?;
            let Some(t) = books.get(&d.product_id) else { return Ok(()) };
            let tick = TickerTick {
                recv_ts_us,
                exch_ts_ms: d.time,
                bid: d.bid,
                ask: d.ask,
                bid_size: d.bid_size,
                ask_size: d.ask_size,
                last: d.last,
                mark_price: d.mark_price,
                index_price: d.index,
                premium: d.premium,
                funding_rate: d.funding_rate,
                funding_rate_prediction: d.funding_rate_prediction,
                relative_funding_rate: d.relative_funding_rate,
                relative_funding_rate_prediction: d.relative_funding_rate_prediction,
                next_funding_ts_ms: d.next_funding_rate_time,
                open_interest: d.open_interest,
                volume_24h: d.volume,
            };
            let rec = Record { market: Market::Futures, instrument: t.instrument.clone(), row: Row::Ticker(tick) };
            let _ = tx.send(rec).await;
        }
        // "trade_snapshot" (recent history, would duplicate rows across
        // reconnects) and "heartbeat" are intentionally ignored.
        _ => {}
    }
    Ok(())
}
