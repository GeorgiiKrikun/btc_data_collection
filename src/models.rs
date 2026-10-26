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

impl Serialize for DepthTick {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut st = s.serialize_struct("DepthTick", 2 + 4 * DEPTH)?;
        st.serialize_field("recv_ts_us", &self.recv_ts_us)?;
        st.serialize_field("exch_ts_us", &self.exch_ts_us)?;
        let mut cols = LEVEL_COLUMNS.iter();
        for (p, q) in self.bids.iter().chain(self.asks.iter()) {
            st.serialize_field(cols.next().unwrap(), p)?;
            st.serialize_field(cols.next().unwrap(), q)?;
        }
        st.end()
    }
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

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Market {
    Spot,
    Futures,
}

impl Market {
    pub fn as_str(self) -> &'static str {
        match self {
            Market::Spot => "spot",
            Market::Futures => "futures",
        }
    }
}

pub enum Row {
    Depth(DepthTick),
    SpotTrade(SpotTradeTick),
    FutTrade(FuturesTradeTick),
    Ticker(TickerTick),
}

impl Row {
    /// File stem the row is written to (`<stem>_YYYYMMDD.csv`).
    pub fn stream(&self) -> &'static str {
        match self {
            Row::Depth(_) => "depth10_tick",
            Row::SpotTrade(_) | Row::FutTrade(_) => "trades_tick",
            Row::Ticker(_) => "ticker",
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
        }
    }
}

pub struct Record {
    pub market: Market,
    /// instrument directory name, e.g. "btc_usd" / "pf_xbtusd"
    pub instrument: Arc<str>,
    pub row: Row,
}
