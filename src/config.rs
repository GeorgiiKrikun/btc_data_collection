//! Collector configuration from environment variables.
//!
//!   ASSETS           (optional)  comma-separated base assets, default "btc,eth,sol,xrp,bnb"
//!   MARKETS          (optional)  Kraken markets, subset of "spot,futures", default both
//!   BINANCE_MARKETS  (optional)  Binance BTCUSDT markets, subset of "spot,futures",
//!                                default both; set to "none" to disable Binance

use crate::models::Market;
use std::sync::Arc;

const DEFAULT_ASSETS: &str = "btc,eth,sol,xrp,bnb";
const DEFAULT_MARKETS: &str = "spot,futures";

/// One exchange instrument: the name Kraken uses on the wire and the
/// filesystem-safe name used for its data directory.
#[derive(Clone)]
pub struct Instrument {
    /// e.g. "BTC/USD" (spot WS v2) or "PF_XBTUSD" (futures)
    pub wire: String,
    /// e.g. "btc_usd" or "pf_xbtusd"
    pub dir: Arc<str>,
}

pub struct Config {
    /// Kraken instruments (Binance is BTCUSDT only and needs none)
    pub spot: Vec<Instrument>,
    pub futures: Vec<Instrument>,
    /// every enabled market, Kraken and Binance
    pub markets: Vec<Market>,
}

impl Config {
    pub fn from_env() -> Config {
        let assets = env_list("ASSETS", DEFAULT_ASSETS);
        let mut markets = parse_markets("MARKETS", Market::KrakenSpot, Market::KrakenFutures);
        markets.extend(parse_markets("BINANCE_MARKETS", Market::BinanceSpot, Market::BinanceFutures));

        let spot = assets.iter().map(|a| spot_instrument(a)).collect();
        let futures = assets.iter().map(|a| futures_instrument(a)).collect();
        Config { spot, futures, markets }
    }
}

/// Reads a "spot,futures" subset; anything else (e.g. "none") enables nothing.
fn parse_markets(key: &str, spot: Market, futures: Market) -> Vec<Market> {
    env_list(key, DEFAULT_MARKETS)
        .iter()
        .filter_map(|m| match m.as_str() {
            "spot" => Some(spot),
            "futures" => Some(futures),
            _ => None,
        })
        .collect()
}

fn env_list(key: &str, default: &str) -> Vec<String> {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_string())
        .split(',')
        .map(|s| s.trim().to_lowercase())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Spot WS v2 uses "BTC/USD" (not Kraken's legacy "XBT").
fn spot_instrument(asset: &str) -> Instrument {
    let base = asset.to_uppercase();
    Instrument {
        wire: format!("{base}/USD"),
        dir: format!("{}_usd", asset).into(),
    }
}

/// Linear multi-collateral perpetuals: "PF_<BASE>USD", with BTC spelled "XBT".
fn futures_instrument(asset: &str) -> Instrument {
    let base = match asset {
        "btc" => "XBT".to_string(),
        other => other.to_uppercase(),
    };
    let wire = format!("PF_{base}USD");
    let dir = wire.to_lowercase().into();
    Instrument { wire, dir }
}
