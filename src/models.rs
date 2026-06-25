use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Deserialize)]
pub struct StreamEnvelope {
    pub stream: String,
    pub data: Value,
}

#[derive(Deserialize)]
pub struct DepthData {
    #[serde(rename = "lastUpdateId")]
    pub last_update_id: u64,
    pub bids: Vec<[String; 2]>,
    pub asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
pub struct AggTradeData {
    #[serde(rename = "E")]
    pub event_ts: i64,
    #[serde(rename = "T")]
    pub trade_ts: i64,
    #[serde(rename = "a")]
    pub agg_trade_id: u64,
    #[serde(rename = "p")]
    pub price: String,
    #[serde(rename = "q")]
    pub qty: String,
    /// true = buyer is maker → taker sell; false = buyer is taker → taker buy
    #[serde(rename = "m")]
    pub is_buyer_maker: bool,
}

#[derive(Serialize, Clone)]
pub struct DepthTick {
    pub recv_ts_us: i64,
    pub last_update_id: u64,
    pub bid1_p: f64,
    pub bid1_q: f64,
    pub bid2_p: f64,
    pub bid2_q: f64,
    pub bid3_p: f64,
    pub bid3_q: f64,
    pub bid4_p: f64,
    pub bid4_q: f64,
    pub bid5_p: f64,
    pub bid5_q: f64,
    pub ask1_p: f64,
    pub ask1_q: f64,
    pub ask2_p: f64,
    pub ask2_q: f64,
    pub ask3_p: f64,
    pub ask3_q: f64,
    pub ask4_p: f64,
    pub ask4_q: f64,
    pub ask5_p: f64,
    pub ask5_q: f64,
}

#[derive(Serialize, Clone)]
pub struct TradeTick {
    pub recv_ts_us: i64,
    pub event_ts_ms: i64,
    pub trade_ts_ms: i64,
    pub agg_trade_id: u64,
    pub price: f64,
    pub qty: f64,
    pub is_buyer_maker: bool,
}
