//! Binance BTCUSDT public market data over combined-stream WebSockets.
//!
//! Spot is one socket (aggTrade + depth5). USDⓈ-M futures needs two, because
//! Binance serves the book only on the `/public` route and trades, mark price
//! and liquidations only on `/market` (the legacy `/stream` URL now delivers
//! just the book). Each socket reconnects on its own.

use crate::models::{
    BinanceAggTrade, BinanceEnvelope, BinanceForceOrder, BinanceFuturesDepth,
    BinanceFuturesDepthTick, BinanceMarkPrice, BinanceSpotDepth, BinanceSpotDepthTick,
    BinanceTradeTick, LiquidationTick, Market, MarkPriceTick, Record, Row, StrLevel,
};
use anyhow::{bail, Result};
use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, LazyLock};
use tokio::sync::mpsc;
use tokio::time::{interval, Duration, Instant};
use tokio_tungstenite::tungstenite::{self, Message};
use tokio_tungstenite::connect_async;
use tracing::{error, info, warn};

/// Instrument directory: `data/binance/<market>/btcusdt/`.
static INSTRUMENT: LazyLock<Arc<str>> = LazyLock::new(|| "btcusdt".into());

/// The heartbeat streams below push every 100ms / 1s, so silence means a dead link.
const MAX_SILENCE: Duration = Duration::from_secs(10);

#[derive(Clone, Copy)]
pub enum Feed {
    Spot,
    /// futures book (`/public` route)
    FuturesPublic,
    /// futures trades, mark price, liquidations (`/market` route)
    FuturesMarket,
}

impl Feed {
    pub fn market(self) -> Market {
        match self {
            Feed::Spot => Market::BinanceSpot,
            Feed::FuturesPublic | Feed::FuturesMarket => Market::BinanceFutures,
        }
    }

    /// Label for this socket's log lines.
    pub fn name(self) -> &'static str {
        match self {
            Feed::Spot => "binance_spot",
            Feed::FuturesPublic => "binance_futures/public",
            Feed::FuturesMarket => "binance_futures/market",
        }
    }

    /// The endpoint, overridable for testing (e.g. a bad URL to check that a
    /// refused connection alerts). An override must carry the same streams.
    fn url(self) -> String {
        let (var, default) = match self {
            Feed::Spot => (
                "BINANCE_SPOT_URL",
                "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@depth5@100ms",
            ),
            Feed::FuturesPublic => (
                "BINANCE_FUTURES_PUBLIC_URL",
                "wss://fstream.binance.com/public/stream?streams=btcusdt@depth10@100ms",
            ),
            Feed::FuturesMarket => (
                "BINANCE_FUTURES_MARKET_URL",
                "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder",
            ),
        };
        std::env::var(var).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| default.into())
    }

    /// The stream the silence watchdog keys on: one that is never legitimately
    /// quiet (not forceOrder, which can be silent for hours, nor aggTrade).
    fn heartbeat_stream(self) -> &'static str {
        match self {
            Feed::Spot => "btcusdt@depth5@100ms",
            Feed::FuturesPublic => "btcusdt@depth10@100ms",
            Feed::FuturesMarket => "btcusdt@markPrice@1s",
        }
    }
}

pub async fn run(feed: Feed, tx: mpsc::Sender<Record>) -> Result<()> {
    let ws_stream = match connect_async(feed.url().as_str()).await {
        Ok((ws, _)) => ws,
        // Binance refuses restricted regions with 451 (or 403) at the handshake.
        // Log every such refusal at ERROR so it alerts right away; the caller
        // keeps retrying with backoff.
        Err(tungstenite::Error::Http(resp)) if matches!(resp.status().as_u16(), 403 | 451) => {
            let status = resp.status().as_u16();
            error!(market = feed.name(), status, "Binance refused the connection with HTTP {status} (geo-blocked?)");
            bail!("handshake refused with HTTP {status}");
        }
        Err(e) => return Err(e.into()),
    };
    info!(market = feed.name(), "Connected to Binance WebSocket");
    let (mut write, mut read) = ws_stream.split();

    let market = feed.market();
    let heartbeat = feed.heartbeat_stream();
    let mut watchdog = interval(Duration::from_secs(1));
    let mut last_beat = Instant::now();

    loop {
        tokio::select! {
            msg = read.next() => {
                let Some(msg) = msg else { break };
                let recv_ts_us = Utc::now().timestamp_micros();
                match msg? {
                    Message::Text(text) => match parse_message(&text, recv_ts_us) {
                        Ok((stream, row)) => {
                            if stream == heartbeat {
                                last_beat = Instant::now();
                            }
                            let rec = Record { market, instrument: INSTRUMENT.clone(), row };
                            let _ = tx.send(rec).await;
                        }
                        Err(e) => warn!(market = feed.name(), "Message error: {e:#}: {text}"),
                    },
                    // Binance drops connections that don't answer its pings.
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    // Binance closes every connection after 24h; the caller
                    // reconnects immediately on a clean close.
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = watchdog.tick() => {
                if last_beat.elapsed() > MAX_SILENCE {
                    bail!("no {heartbeat} from Binance for {}s", last_beat.elapsed().as_secs());
                }
            }
        }
    }
    Ok(())
}

/// Turns one combined-stream message into its stream name and CSV row.
fn parse_message(text: &str, recv_ts_us: i64) -> Result<(&str, Row)> {
    let env: BinanceEnvelope = serde_json::from_str(text)?;
    let data = env.data.get();
    let row = match env.stream {
        "btcusdt@aggTrade" => {
            let d: BinanceAggTrade = serde_json::from_str(data)?;
            Row::BinanceTrade(BinanceTradeTick {
                recv_ts_us,
                event_ts_ms: d.event_ts,
                trade_ts_ms: d.trade_ts,
                agg_trade_id: d.agg_trade_id,
                price: d.price.0,
                qty: d.qty.0,
                is_buyer_maker: d.is_buyer_maker,
            })
        }
        "btcusdt@depth5@100ms" => {
            let d: BinanceSpotDepth = serde_json::from_str(data)?;
            Row::BinanceSpotDepth(BinanceSpotDepthTick {
                recv_ts_us,
                last_update_id: d.last_update_id,
                bids: levels(&d.bids),
                asks: levels(&d.asks),
            })
        }
        "btcusdt@depth10@100ms" => {
            let d: BinanceFuturesDepth = serde_json::from_str(data)?;
            Row::BinanceFutDepth(BinanceFuturesDepthTick {
                recv_ts_us,
                event_ts_ms: d.event_ts,
                transaction_ts_ms: d.transaction_ts,
                last_update_id: d.last_update_id,
                bids: levels(&d.bids),
                asks: levels(&d.asks),
            })
        }
        "btcusdt@markPrice@1s" => {
            let d: BinanceMarkPrice = serde_json::from_str(data)?;
            Row::MarkPrice(MarkPriceTick {
                recv_ts_us,
                event_ts_ms: d.event_ts,
                mark_price: d.mark_price.0,
                index_price: d.index_price.0,
                est_settle_price: d.est_settle_price.0,
                funding_rate: d.funding_rate.0,
                next_funding_ts_ms: d.next_funding_ts,
            })
        }
        "btcusdt@forceOrder" => {
            let d: BinanceForceOrder = serde_json::from_str(data)?;
            let o = d.order;
            Row::Liquidation(LiquidationTick {
                recv_ts_us,
                event_ts_ms: d.event_ts,
                trade_ts_ms: o.trade_ts,
                side: o.side,
                order_type: o.order_type,
                time_in_force: o.time_in_force,
                orig_qty: o.orig_qty.0,
                price: o.price.0,
                avg_price: o.avg_price.0,
                status: o.status,
                last_filled_qty: o.last_filled_qty.0,
                filled_acc_qty: o.filled_acc_qty.0,
            })
        }
        other => bail!("unexpected stream {other:?}"),
    };
    Ok((env.stream, row))
}

/// Top `N` levels as numbers; missing levels are 0.
fn levels<const N: usize>(src: &[StrLevel]) -> [(f64, f64); N] {
    let mut out = [(0.0, 0.0); N];
    for (slot, (p, q)) in out.iter_mut().zip(src) {
        *slot = (p.0, q.0);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One real captured message per stream (2026-10-27), in this order: spot
    /// depth5, spot aggTrade, futures depth10, futures aggTrade, markPrice, forceOrder.
    const CAPTURE: &str = include_str!("testdata/binance_streams.jsonl");
    const RECV: i64 = 1_793_108_768_250_000;

    /// Parses `line` and renders it as a CSV file would hold it: header + row.
    fn csv_of(line: &str) -> (String, String) {
        let (stream, row) = parse_message(line, RECV).unwrap();
        let mut w = csv::Writer::from_writer(Vec::new());
        w.serialize(row).unwrap();
        let out = String::from_utf8(w.into_inner().unwrap()).unwrap();
        let (header, row) = out.trim_end().split_once('\n').unwrap();
        assert!(stream.starts_with("btcusdt@"));
        (header.to_string(), row.to_string())
    }

    fn fixture(i: usize) -> &'static str {
        CAPTURE.lines().nth(i).unwrap()
    }

    #[test]
    fn spot_depth5() {
        let (header, row) = csv_of(fixture(0));
        assert_eq!(header, "recv_ts_us,last_update_id,bid1_p,bid1_q,bid2_p,bid2_q,bid3_p,bid3_q,bid4_p,bid4_q,bid5_p,bid5_q,ask1_p,ask1_q,ask2_p,ask2_q,ask3_p,ask3_q,ask4_p,ask4_q,ask5_p,ask5_q");
        assert_eq!(row, "1793108768250000,101964840065,80188.0,1.64475,80187.99,0.00056,80187.96,0.00007,80186.81,0.00007,80186.01,0.00014,80188.01,7.23872,80188.5,0.002,80189.0,0.00007,80189.29,0.00014,80189.3,0.01451");
    }

    #[test]
    fn spot_agg_trade() {
        let (header, row) = csv_of(fixture(1));
        assert_eq!(header, "recv_ts_us,event_ts_ms,trade_ts_ms,agg_trade_id,price,qty,is_buyer_maker");
        assert_eq!(row, "1793108768250000,1793108751679,1793108751678,4099321280,80188.0,0.01124,true");
    }

    #[test]
    fn futures_depth10() {
        let (header, row) = csv_of(fixture(2));
        let levels: Vec<String> = ["bid", "ask"]
            .iter()
            .flat_map(|s| (1..=10).flat_map(move |i| [format!("{s}{i}_p"), format!("{s}{i}_q")]))
            .collect();
        assert_eq!(header, format!("recv_ts_us,event_ts_ms,transaction_ts_ms,last_update_id,{}", levels.join(",")));
        assert_eq!(row, "1793108768250000,1793108768239,1793108768237,11937835189113,\
            80172.2,14.0,80172.1,2.581,80172.0,1.277,80171.9,2.078,80171.8,0.002,80171.6,0.004,80171.4,0.004,80171.3,0.03,80171.2,0.077,80170.8,0.002,\
            80172.3,1.577,80172.4,0.002,80172.5,0.035,80173.0,0.002,80173.1,0.05,80173.2,0.002,80173.3,0.001,80174.0,0.001,80174.3,0.001,80174.6,0.002");
    }

    #[test]
    fn futures_depth_with_missing_levels_pads_zeros() {
        // The futures depth10 fixture cut to 2 bids / 1 ask (real messages for
        // BTCUSDT always carry 10 levels; thin books elsewhere don't).
        let line = r#"{"stream":"btcusdt@depth10@100ms","data":{"e":"depthUpdate","E":1793108768239,"T":1793108768237,"s":"BTCUSDT","ps":"BTCUSDT","U":11937835174916,"u":11937835189113,"pu":11937835174862,"b":[["80172.20","14.000"],["80172.10","2.581"]],"a":[["80172.30","1.577"]],"st":1}}"#;
        let (_, row) = csv_of(line);
        let fields: Vec<&str> = row.split(',').collect();
        assert_eq!(fields.len(), 4 + 40);
        assert_eq!(fields[4..8], ["80172.2", "14.0", "80172.1", "2.581"]);
        assert!(fields[8..24].iter().all(|f| *f == "0.0"), "bid3..bid10 are 0");
        assert_eq!(fields[24..26], ["80172.3", "1.577"]);
        assert!(fields[26..].iter().all(|f| *f == "0.0"), "ask2..ask10 are 0");
    }

    #[test]
    fn futures_agg_trade() {
        let (header, row) = csv_of(fixture(3));
        assert_eq!(header, "recv_ts_us,event_ts_ms,trade_ts_ms,agg_trade_id,price,qty,is_buyer_maker");
        assert_eq!(row, "1793108768250000,1793108768165,1793108768014,3497903394,80172.2,0.062,true");
    }

    #[test]
    fn mark_price() {
        let (header, row) = csv_of(fixture(4));
        assert_eq!(header, "recv_ts_us,event_ts_ms,mark_price,index_price,est_settle_price,funding_rate,next_funding_ts_ms");
        assert_eq!(row, "1793108768250000,1793108769000,80172.2,80200.16152174,80203.87845543,0.0001,1793116800000");
    }

    #[test]
    fn force_order() {
        let (header, row) = csv_of(fixture(5));
        assert_eq!(header, "recv_ts_us,event_ts_ms,trade_ts_ms,side,order_type,time_in_force,orig_qty,price,avg_price,status,last_filled_qty,filled_acc_qty");
        assert_eq!(row, "1793108768250000,1793108976215,1793108976179,SELL,LIMIT,IOC,0.133,79795.6,80105.1,FILLED,0.133,0.133");
    }

    #[test]
    fn heartbeat_streams_match_parsed_names() {
        for (feed, line) in [(Feed::Spot, 0), (Feed::FuturesPublic, 2), (Feed::FuturesMarket, 4)] {
            let (stream, _) = parse_message(fixture(line), RECV).unwrap();
            assert_eq!(stream, feed.heartbeat_stream());
        }
    }

    #[test]
    fn unknown_stream_is_an_error() {
        assert!(parse_message(r#"{"result":null,"id":1}"#, RECV).is_err());
        assert!(parse_message(r#"{"stream":"ethusdt@aggTrade","data":{}}"#, RECV).is_err());
    }
}
