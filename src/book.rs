//! Local L2 order book, maintained from Kraken snapshot + incremental updates
//! and sampled into fixed-width `DepthTick` rows.

use crate::models::{DepthTick, Level, DEPTH};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::BTreeMap;
use std::fmt;

/// The local book no longer matches the exchange (checksum mismatch or a
/// sequence gap). The connection must be dropped and rebuilt from a fresh
/// snapshot.
#[derive(Debug)]
pub struct Desync(pub String);

impl fmt::Display for Desync {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "book desync: {}", self.0)
    }
}

impl std::error::Error for Desync {}

#[derive(Clone, Copy)]
pub enum Side {
    Bid,
    Ask,
}

pub struct Book {
    /// ascending by price; best bid is the last entry
    bids: BTreeMap<Decimal, Decimal>,
    /// ascending by price; best ask is the first entry
    asks: BTreeMap<Decimal, Decimal>,
    /// `Some(n)`: keep only the best `n` levels per side (Kraken spot sends no
    /// deletes for levels that fall out of the subscribed depth).
    max_depth: Option<usize>,
    /// changed since the last sample
    dirty: bool,
    exch_ts_us: i64,
}

impl Book {
    pub fn new(max_depth: Option<usize>) -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            max_depth,
            dirty: false,
            exch_ts_us: 0,
        }
    }

    pub fn clear(&mut self) {
        self.bids.clear();
        self.asks.clear();
    }

    /// Sets one price level; a zero quantity removes it.
    pub fn set(&mut self, side: Side, price: Decimal, qty: Decimal) {
        let levels = match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        };
        if qty.is_zero() {
            levels.remove(&price);
        } else {
            levels.insert(price, qty);
        }
    }

    pub fn apply(&mut self, bids: &[Level], asks: &[Level]) {
        for l in bids {
            self.set(Side::Bid, l.price, l.qty);
        }
        for l in asks {
            self.set(Side::Ask, l.price, l.qty);
        }
        self.truncate();
    }

    /// Records that the book changed at exchange time `exch_ts_us`.
    pub fn touch(&mut self, exch_ts_us: i64) {
        self.dirty = true;
        self.exch_ts_us = exch_ts_us;
    }

    fn truncate(&mut self) {
        if let Some(n) = self.max_depth {
            while self.bids.len() > n {
                self.bids.pop_first();
            }
            while self.asks.len() > n {
                self.asks.pop_last();
            }
        }
    }

    /// Kraken spot WS v2 book checksum: CRC32 over the top 10 asks (low → high)
    /// then the top 10 bids (high → low), each level as price then qty with the
    /// decimal point and leading zeros removed (trailing zeros are kept, which
    /// is why levels are stored as exact `Decimal`s with their wire scale).
    pub fn checksum(&self) -> u32 {
        fn push(buf: &mut String, d: &Decimal) {
            let s = d.to_string().replace('.', "");
            buf.push_str(s.trim_start_matches('0'));
        }
        let mut buf = String::with_capacity(512);
        for (p, q) in self.asks.iter().take(10) {
            push(&mut buf, p);
            push(&mut buf, q);
        }
        for (p, q) in self.bids.iter().rev().take(10) {
            push(&mut buf, p);
            push(&mut buf, q);
        }
        crc32fast::hash(buf.as_bytes())
    }

    /// Returns the top `DEPTH` levels if the book changed since the last call
    /// (missing levels are zero-padded).
    pub fn take_sample(&mut self, recv_ts_us: i64) -> Option<DepthTick> {
        if !self.dirty {
            return None;
        }
        self.dirty = false;
        Some(DepthTick {
            recv_ts_us,
            exch_ts_us: self.exch_ts_us,
            bids: top(self.bids.iter().rev()),
            asks: top(self.asks.iter()),
        })
    }
}

fn top<'a>(levels: impl Iterator<Item = (&'a Decimal, &'a Decimal)>) -> [(f64, f64); DEPTH] {
    let mut out = [(0.0, 0.0); DEPTH];
    for (slot, (p, q)) in out.iter_mut().zip(levels) {
        *slot = (p.to_f64().unwrap_or(0.0), q.to_f64().unwrap_or(0.0));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SpotBookData, SpotMsg};

    /// A live capture of a BTC/USD depth-10 subscription: one snapshot followed
    /// by updates (including deletes and levels pushed out of the top 10).
    /// Every message's checksum must match the locally maintained book.
    #[test]
    fn spot_checksum_matches_live_capture() {
        let capture = include_str!("testdata/kraken_spot_book_btcusd.jsonl");
        let mut book = Book::new(Some(DEPTH));
        let mut checked = 0;
        for line in capture.lines() {
            let msg: SpotMsg<SpotBookData> = serde_json::from_str(line).unwrap();
            for d in &msg.data {
                if line.contains(r#""type":"snapshot""#) {
                    book.clear();
                }
                book.apply(&d.bids, &d.asks);
                assert_eq!(book.checksum(), d.checksum, "checksum mismatch at: {line}");
                checked += 1;
            }
        }
        assert!(checked > 10);
    }

    #[test]
    fn sample_orders_levels_best_first_and_pads() {
        let mut book = Book::new(None);
        book.set(Side::Bid, Decimal::new(100, 0), Decimal::new(1, 0));
        book.set(Side::Bid, Decimal::new(101, 0), Decimal::new(2, 0));
        book.set(Side::Ask, Decimal::new(103, 0), Decimal::new(3, 0));
        book.set(Side::Ask, Decimal::new(102, 0), Decimal::new(4, 0));
        book.set(Side::Ask, Decimal::new(102, 0), Decimal::ZERO); // delete
        assert!(book.take_sample(1).is_none(), "untouched book must not sample");
        book.touch(42);
        let t = book.take_sample(7).unwrap();
        assert_eq!((t.recv_ts_us, t.exch_ts_us), (7, 42));
        assert_eq!(t.bids[0], (101.0, 2.0));
        assert_eq!(t.bids[1], (100.0, 1.0));
        assert_eq!(t.bids[2], (0.0, 0.0));
        assert_eq!(t.asks[0], (103.0, 3.0));
        assert!(book.take_sample(8).is_none(), "sample clears the dirty flag");
    }
}
