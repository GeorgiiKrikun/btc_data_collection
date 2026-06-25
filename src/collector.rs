use crate::models::{AggTradeData, DepthData, DepthTick, StreamEnvelope, TradeTick};
use anyhow::Result;
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{info, warn};

const WS_URL: &str =
    "wss://stream.binance.com:9443/stream?streams=btcusdt@depth5@100ms/btcusdt@aggTrade";

pub async fn run(
    depth_tx: mpsc::Sender<DepthTick>,
    trade_tx: mpsc::Sender<TradeTick>,
) -> Result<()> {
    let (ws_stream, _) = connect_async(WS_URL).await?;
    info!("Connected to Binance WebSocket");
    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        let recv_ts_us = Utc::now().timestamp_micros();
        match msg? {
            Message::Text(text) => {
                if let Err(e) = handle_message(&text, recv_ts_us, &depth_tx, &trade_tx).await {
                    warn!("Message error: {e}");
                }
            }
            Message::Ping(payload) => {
                write.send(Message::Pong(payload)).await?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

async fn handle_message(
    text: &str,
    recv_ts_us: i64,
    depth_tx: &mpsc::Sender<DepthTick>,
    trade_tx: &mpsc::Sender<TradeTick>,
) -> Result<()> {
    let env: StreamEnvelope = serde_json::from_str(text)?;

    if env.stream.contains("depth5") {
        let depth: DepthData = serde_json::from_value(env.data)?;
        let tick = parse_depth(recv_ts_us, &depth)?;
        let _ = depth_tx.send(tick).await;
    } else if env.stream.contains("aggTrade") {
        let trade: AggTradeData = serde_json::from_value(env.data)?;
        let tick = TradeTick {
            recv_ts_us,
            event_ts_ms: trade.event_ts,
            trade_ts_ms: trade.trade_ts,
            agg_trade_id: trade.agg_trade_id,
            price: trade.price.parse()?,
            qty: trade.qty.parse()?,
            is_buyer_maker: trade.is_buyer_maker,
        };
        let _ = trade_tx.send(tick).await;
    }

    Ok(())
}

fn parse_depth(recv_ts_us: i64, depth: &DepthData) -> Result<DepthTick> {
    fn level(levels: &[[String; 2]], i: usize) -> (f64, f64) {
        levels
            .get(i)
            .and_then(|[p, q]| Some((p.parse().ok()?, q.parse().ok()?)))
            .unwrap_or((0.0, 0.0))
    }

    let (bid1_p, bid1_q) = level(&depth.bids, 0);
    let (bid2_p, bid2_q) = level(&depth.bids, 1);
    let (bid3_p, bid3_q) = level(&depth.bids, 2);
    let (bid4_p, bid4_q) = level(&depth.bids, 3);
    let (bid5_p, bid5_q) = level(&depth.bids, 4);
    let (ask1_p, ask1_q) = level(&depth.asks, 0);
    let (ask2_p, ask2_q) = level(&depth.asks, 1);
    let (ask3_p, ask3_q) = level(&depth.asks, 2);
    let (ask4_p, ask4_q) = level(&depth.asks, 3);
    let (ask5_p, ask5_q) = level(&depth.asks, 4);

    Ok(DepthTick {
        recv_ts_us,
        last_update_id: depth.last_update_id,
        bid1_p, bid1_q,
        bid2_p, bid2_q,
        bid3_p, bid3_q,
        bid4_p, bid4_q,
        bid5_p, bid5_q,
        ask1_p, ask1_q,
        ask2_p, ask2_q,
        ask3_p, ask3_q,
        ask4_p, ask4_q,
        ask5_p, ask5_q,
    })
}
