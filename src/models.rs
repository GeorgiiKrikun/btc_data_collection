use rust_decimal::Decimal;
use serde::de::Error as _;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::value::RawValue;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};

/// Number of book levels per side written to `depth10_tick` files.
pub const DEPTH: usize = 10;

// ---------------------------------------------------------------------------
// Kraken wire format
// ---------------------------------------------------------------------------

/// Decodes a JSON number from its raw text into an exact `Decimal`, keeping
/// trailing zeros (Kraken's spot book checksum depends on them).
fn de_decimal<'de, D: Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
    let raw = <&RawValue>::deserialize(d)?;
    let text = raw.get().trim_matches('"');
    Decimal::from_str(text)
        .or_else(|_| Decimal::from_scientific(text))
        .map_err(D::Error::custom)
}

#[derive(Deserialize)]
pub struct Level {
    #[serde(deserialize_with = "de_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "de_decimal")]
    pub qty: Decimal,
}

/// Just enough of any spot WS v2 message to dispatch on.
#[derive(Deserialize)]
pub struct SpotHead<'a> {
    #[serde(borrow)]
    pub channel: Option<&'a str>,
    #[serde(borrow, rename = "type")]
    pub kind: Option<&'a str>,
    #[serde(borrow)]
    pub method: Option<&'a str>,
    pub success: Option<bool>,
    pub error: Option<String>,
    pub result: Option<serde_json::Value>,
}

#[derive(Deserialize)]
pub struct SpotMsg<T> {
    pub data: Vec<T>,
}

#[derive(Deserialize)]
pub struct SpotBookData {
    pub symbol: String,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub checksum: u32,
    pub timestamp: String,
}

#[derive(Deserialize)]
pub struct SpotTradeData {
    pub symbol: String,
    /// taker side: "buy" | "sell"
    pub side: String,
    pub price: f64,
    pub qty: f64,
    /// "limit" | "market"
    pub ord_type: String,
    pub trade_id: u64,
    pub timestamp: String,
}

/// Just enough of any futures WS v1 message to dispatch on.
#[derive(Deserialize)]
pub struct FuturesHead<'a> {
    #[serde(borrow)]
    pub event: Option<&'a str>,
    #[serde(borrow)]
    pub feed: Option<&'a str>,
}

#[derive(Deserialize)]
pub struct FuturesBookSnapshot {
    pub product_id: String,
    pub timestamp: i64,
    pub seq: u64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

#[derive(Deserialize)]
pub struct FuturesBookDelta {
    pub product_id: String,
    /// "buy" = bid level, "sell" = ask level
    pub side: String,
    pub seq: u64,
    #[serde(deserialize_with = "de_decimal")]
    pub price: Decimal,
    #[serde(deserialize_with = "de_decimal")]
    pub qty: Decimal,
    pub timestamp: i64,
}

#[derive(Deserialize)]
pub struct FuturesTradeData {
    pub product_id: String,
    pub uid: String,
    /// taker side: "buy" | "sell"
    pub side: String,
    /// "fill" | "liquidation" | "termination" | "block"
    #[serde(rename = "type")]
    pub trade_type: String,
    pub seq: u64,
    pub time: i64,
    pub qty: f64,
    pub price: f64,
}

#[derive(Deserialize)]
pub struct FuturesTickerData {
    pub product_id: String,
    pub time: i64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_size: Option<f64>,
    pub ask_size: Option<f64>,
    pub last: Option<f64>,
    #[serde(rename = "markPrice")]
    pub mark_price: Option<f64>,
    pub index: Option<f64>,
    pub premium: Option<f64>,
    pub funding_rate: Option<f64>,
    pub funding_rate_prediction: Option<f64>,
    pub relative_funding_rate: Option<f64>,
    pub relative_funding_rate_prediction: Option<f64>,
    /// absolute epoch ms of the next funding event
    pub next_funding_rate_time: Option<i64>,
    #[serde(rename = "openInterest")]
    pub open_interest: Option<f64>,
    pub volume: Option<f64>,
}

// ---------------------------------------------------------------------------
// Binance wire format (combined streams; prices and quantities are strings)
// ---------------------------------------------------------------------------

/// Combined-stream envelope: `{"stream": "<name>", "data": {…}}`.
#[derive(Deserialize)]
pub struct BinanceEnvelope<'a> {
    #[serde(borrow)]
    pub stream: &'a str,
    #[serde(borrow)]
    pub data: &'a RawValue,
}

/// A decimal string such as `"80188.00000000"`, parsed to a number.
#[derive(Deserialize)]
pub struct StrNum(#[serde(deserialize_with = "de_str_f64")] pub f64);

fn de_str_f64<'de, D: Deserializer<'de>>(d: D) -> Result<f64, D::Error> {
    let s = <&str>::deserialize(d)?;
    s.parse().map_err(D::Error::custom)
}

/// `[price, qty]`
pub type StrLevel = (StrNum, StrNum);

/// Spot partial depth (`<symbol>@depth5@100ms`).
#[derive(Deserialize)]
pub struct BinanceSpotDepth {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<StrLevel>,
    pub asks: Vec<StrLevel>,
}

/// Futures partial depth (`<symbol>@depth10@100ms`).
#[derive(Deserialize)]
pub struct BinanceFuturesDepth {
    #[serde(rename = "E")]
    pub event_ts: i64,
    #[serde(rename = "T")]
    pub transaction_ts: i64,
    #[serde(rename = "u")]
    pub last_update_id: u64,
    #[serde(rename = "b")]
    pub bids: Vec<StrLevel>,
    #[serde(rename = "a")]
    pub asks: Vec<StrLevel>,
}

/// `<symbol>@aggTrade` (same fields on spot and futures).
#[derive(Deserialize)]
pub struct BinanceAggTrade {
    #[serde(rename = "E")]
    pub event_ts: i64,
    #[serde(rename = "T")]
    pub trade_ts: i64,
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: StrNum,
    #[serde(rename = "q")]
    pub qty: StrNum,
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

/// `<symbol>@markPrice@1s`
#[derive(Deserialize)]
pub struct BinanceMarkPrice {
    #[serde(rename = "E")]
    pub event_ts: i64,
    #[serde(rename = "p")]
    pub mark_price: StrNum,
    #[serde(rename = "i")]
    pub index_price: StrNum,
    #[serde(rename = "P")]
    pub est_settle_price: StrNum,
    #[serde(rename = "r")]
    pub funding_rate: StrNum,
    #[serde(rename = "T")]
    pub next_funding_ts: i64,
}

/// `<symbol>@forceOrder`
#[derive(Deserialize)]
pub struct BinanceForceOrder {
    #[serde(rename = "E")]
    pub event_ts: i64,
    #[serde(rename = "o")]
    pub order: BinanceForceOrderData,
}

#[derive(Deserialize)]
pub struct BinanceForceOrderData {
    #[serde(rename = "T")]
    pub trade_ts: i64,
    /// side of the liquidation order: "SELL" = a long was liquidated
    #[serde(rename = "S")]
    pub side: String,
    #[serde(rename = "o")]
    pub order_type: String,
    #[serde(rename = "f")]
    pub time_in_force: String,
    #[serde(rename = "q")]
    pub orig_qty: StrNum,
    #[serde(rename = "p")]
    pub price: StrNum,
    #[serde(rename = "ap")]
    pub avg_price: StrNum,
    #[serde(rename = "X")]
    pub status: String,
    #[serde(rename = "l")]
    pub last_filled_qty: StrNum,
    #[serde(rename = "z")]
    pub filled_acc_qty: StrNum,
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

/// Top-of-book sample: `recv_ts_us, exch_ts_us, bid1_p, bid1_q … bid10_q, ask1_p … ask10_q`.
#[derive(Clone)]
pub struct DepthTick {
    pub recv_ts_us: i64,
    /// exchange timestamp of the last update applied before the sample
    pub exch_ts_us: i64,
    pub bids: [(f64, f64); DEPTH],
    pub asks: [(f64, f64); DEPTH],
}

static LEVEL_COLUMNS: LazyLock<Vec<&'static str>> = LazyLock::new(|| {
    let mut cols = Vec::with_capacity(4 * DEPTH);
    for side in ["bid", "ask"] {
        for i in 1..=DEPTH {
            for field in ["p", "q"] {
                cols.push(&*format!("{side}{i}_{field}").leak());
            }
        }
    }
    cols
});

/// Writes `bid1_p, bid1_q … bid{N}_q, ask1_p … ask{N}_q` (N ≤ DEPTH).
fn serialize_levels<S: SerializeStruct, const N: usize>(
    st: &mut S,
    bids: &[(f64, f64); N],
    asks: &[(f64, f64); N],
) -> Result<(), S::Error> {
    for (side, levels) in [bids, asks].into_iter().enumerate() {
        let cols = &LEVEL_COLUMNS[side * 2 * DEPTH..];
        for (i, (p, q)) in levels.iter().enumerate() {
            st.serialize_field(cols[2 * i], p)?;
            st.serialize_field(cols[2 * i + 1], q)?;
        }
    }
    Ok(())
}

impl Serialize for DepthTick {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("DepthTick", 2 + 4 * DEPTH)?;
        st.serialize_field("recv_ts_us", &self.recv_ts_us)?;
        st.serialize_field("exch_ts_us", &self.exch_ts_us)?;
        serialize_levels(&mut st, &self.bids, &self.asks)?;
        st.end()
    }
}

/// Binance spot partial book, same columns as the legacy Binance files:
/// `recv_ts_us, last_update_id, bid1_p, bid1_q … bid5_q, ask1_p … ask5_q`.
pub struct BinanceSpotDepthTick {
    pub recv_ts_us: i64,
    pub last_update_id: u64,
    pub bids: [(f64, f64); 5],
    pub asks: [(f64, f64); 5],
}

impl Serialize for BinanceSpotDepthTick {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("BinanceSpotDepthTick", 2 + 4 * 5)?;
        st.serialize_field("recv_ts_us", &self.recv_ts_us)?;
        st.serialize_field("last_update_id", &self.last_update_id)?;
        serialize_levels(&mut st, &self.bids, &self.asks)?;
        st.end()
    }
}

/// Binance futures partial book: `recv_ts_us, event_ts_ms, transaction_ts_ms,
/// last_update_id, bid1_p, bid1_q … bid10_q, ask1_p … ask10_q`.
pub struct BinanceFuturesDepthTick {
    pub recv_ts_us: i64,
    pub event_ts_ms: i64,
    pub transaction_ts_ms: i64,
    pub last_update_id: u64,
    pub bids: [(f64, f64); DEPTH],
    pub asks: [(f64, f64); DEPTH],
}

impl Serialize for BinanceFuturesDepthTick {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("BinanceFuturesDepthTick", 4 + 4 * DEPTH)?;
        st.serialize_field("recv_ts_us", &self.recv_ts_us)?;
        st.serialize_field("event_ts_ms", &self.event_ts_ms)?;
        st.serialize_field("transaction_ts_ms", &self.transaction_ts_ms)?;
        st.serialize_field("last_update_id", &self.last_update_id)?;
        serialize_levels(&mut st, &self.bids, &self.asks)?;
        st.end()
    }
}

/// Binance aggTrade (spot and futures), same columns as the legacy Binance files.
#[derive(Serialize)]
pub struct BinanceTradeTick {
    pub recv_ts_us: i64,
    pub event_ts_ms: i64,
    pub trade_ts_ms: i64,
    pub agg_trade_id: u64,
    pub price: f64,
    pub qty: f64,
    pub is_buyer_maker: bool,
}

#[derive(Serialize)]
pub struct MarkPriceTick {
    pub recv_ts_us: i64,
    pub event_ts_ms: i64,
    pub mark_price: f64,
    pub index_price: f64,
    pub est_settle_price: f64,
    pub funding_rate: f64,
    pub next_funding_ts_ms: i64,
}

#[derive(Serialize)]
pub struct LiquidationTick {
    pub recv_ts_us: i64,
    pub event_ts_ms: i64,
    pub trade_ts_ms: i64,
    /// side of the liquidation order: "SELL" = a long was liquidated, "BUY" = a short
    pub side: String,
    pub order_type: String,
    pub time_in_force: String,
    pub orig_qty: f64,
    pub price: f64,
    pub avg_price: f64,
    pub status: String,
    pub last_filled_qty: f64,
    pub filled_acc_qty: f64,
}

#[derive(Serialize, Clone)]
pub struct SpotTradeTick {
    pub recv_ts_us: i64,
    pub exch_ts_us: i64,
    pub trade_id: u64,
    pub price: f64,
    pub qty: f64,
    /// taker side
    pub side: String,
    pub ord_type: String,
}

#[derive(Serialize, Clone)]
pub struct FuturesTradeTick {
    pub recv_ts_us: i64,
    pub exch_ts_ms: i64,
    pub seq: u64,
    pub uid: String,
    pub price: f64,
    pub qty: f64,
    /// taker side
    pub side: String,
    /// "fill" | "liquidation" | "termination" | "block"
    pub trade_type: String,
}

#[derive(Serialize, Clone)]
pub struct TickerTick {
    pub recv_ts_us: i64,
    pub exch_ts_ms: i64,
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub bid_size: Option<f64>,
    pub ask_size: Option<f64>,
    pub last: Option<f64>,
    pub mark_price: Option<f64>,
    pub index_price: Option<f64>,
    pub premium: Option<f64>,
    pub funding_rate: Option<f64>,
    pub funding_rate_prediction: Option<f64>,
    pub relative_funding_rate: Option<f64>,
    pub relative_funding_rate_prediction: Option<f64>,
    pub next_funding_ts_ms: Option<i64>,
    pub open_interest: Option<f64>,
    pub volume_24h: Option<f64>,
}

// ---------------------------------------------------------------------------
// Collector → writer channel
// ---------------------------------------------------------------------------

/// One exchange market. Each runs its own connection(s) and has its own data
/// directory and log label.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Market {
    KrakenSpot,
    KrakenFutures,
    BinanceSpot,
    BinanceFutures,
}

impl Market {
    /// Label in logs and in the writer's "market tick" events (read by logwatch).
    pub fn as_str(self) -> &'static str {
        match self {
            Market::KrakenSpot => "kraken_spot",
            Market::KrakenFutures => "kraken_futures",
            Market::BinanceSpot => "binance_spot",
            Market::BinanceFutures => "binance_futures",
        }
    }

    /// Data directory under `data/`.
    pub fn dir(self) -> &'static str {
        match self {
            Market::KrakenSpot => "kraken/spot",
            Market::KrakenFutures => "kraken/futures",
            Market::BinanceSpot => "binance/spot",
            Market::BinanceFutures => "binance/futures",
        }
    }
}

pub enum Row {
    Depth(DepthTick),
    SpotTrade(SpotTradeTick),
    FutTrade(FuturesTradeTick),
    Ticker(TickerTick),
    BinanceSpotDepth(BinanceSpotDepthTick),
    BinanceFutDepth(BinanceFuturesDepthTick),
    BinanceTrade(BinanceTradeTick),
    MarkPrice(MarkPriceTick),
    Liquidation(LiquidationTick),
}

impl Row {
    /// File stem the row is written to (`<stem>_YYYYMMDD.csv`).
    pub fn stream(&self) -> &'static str {
        match self {
            Row::Depth(_) | Row::BinanceFutDepth(_) => "depth10_tick",
            Row::BinanceSpotDepth(_) => "depth5_tick",
            Row::SpotTrade(_) | Row::FutTrade(_) | Row::BinanceTrade(_) => "trades_tick",
            Row::Ticker(_) => "ticker",
            Row::MarkPrice(_) => "mark_price",
            Row::Liquidation(_) => "liquidations",
        }
    }
}

/// Every file holds a single row variant, so delegating keeps the CSV header
/// derived from the concrete struct.
impl Serialize for Row {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self {
            Row::Depth(t) => t.serialize(s),
            Row::SpotTrade(t) => t.serialize(s),
            Row::FutTrade(t) => t.serialize(s),
            Row::Ticker(t) => t.serialize(s),
            Row::BinanceSpotDepth(t) => t.serialize(s),
            Row::BinanceFutDepth(t) => t.serialize(s),
            Row::BinanceTrade(t) => t.serialize(s),
            Row::MarkPrice(t) => t.serialize(s),
            Row::Liquidation(t) => t.serialize(s),
        }
    }
}

pub struct Record {
    pub market: Market,
    /// instrument directory name, e.g. "btc_usd" / "pf_xbtusd" / "btcusdt"
    pub instrument: Arc<str>,
    pub row: Row,
}
