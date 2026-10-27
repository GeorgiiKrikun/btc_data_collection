# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build (compiles all binaries: quant_2 collector, uploader, logwatch)
cargo build --release

# Unit tests (Kraken book checksum + Binance row parsing against live captures, logwatch parsing)
cargo test --release

# Run the collector locally (data written to ./data/{kraken,binance}/ relative to the cwd)
cargo run --release --bin quant_2

# Collect a subset (defaults: ASSETS=btc,eth,sol,xrp,bnb  MARKETS=spot,futures  BINANCE_MARKETS=spot,futures)
ASSETS=btc,eth MARKETS=futures BINANCE_MARKETS=none cargo run --release --bin quant_2

# Run the S3 uploader locally (reads config from env; see .env.example)
S3_BUCKET=... S3_ENDPOINT_URL=... cargo run --release --bin uploader

# Control log level (default: info)
RUST_LOG=quant_2=debug cargo run --release --bin quant_2

# Run in Docker (collector + uploader + logwatch, production mode, auto-restarts)
cp .env.example .env   # then fill in S3 creds/bucket
docker compose up -d --build
docker compose logs -f
docker compose down
```

Container users: quant2 and uploader run as the image's build-arg user, default UID 1000 / GID 100. `./data` must be writable by it (`chown -R 1000:100 data`), and `secrets/s3_credentials` readable by it. Don't use a `UID=$(id -u) …` prefix: `UID` is a read-only variable in bash. To build for another user, pass `docker compose build --build-arg UID=… --build-arg GID=…`. logwatch runs as root (`user: "0:0"` in compose) because it needs the Docker socket.

## Architecture

A Tokio async service that streams market data from **Kraken** (spot + linear perpetual futures, several assets) and **Binance** (BTCUSDT spot + USDⓈ-M futures, public data only) over WebSocket and writes it to date-partitioned CSV files under `./data/kraken/` and `./data/binance/`. Both venues run in one process, so `recv_ts_us` is directly comparable across them. Trading is meant for Kraken perps; Binance is recorded as the price-discovery / liquidation signal source. (Until Oct 2026 only Binance spot BTCUSDT was collected; those files remain in the bucket under `btcusdt/`.)

**Data flow:**

```
Kraken spot WS v2           →  kraken_spot::run()               ┐
Kraken Futures WS v1        →  kraken_futures::run()            │
Binance spot                →  binance::run(Feed::Spot)          ├→  mpsc<Record>  →  writer::writer_task()  →  CSV files
Binance futures /public     →  binance::run(Feed::FuturesPublic) │
Binance futures /market     →  binance::run(Feed::FuturesMarket) ┘
```

- `main.rs` — installs the rustls crypto provider, reads `config.rs`, creates one `mpsc::channel::<Record>`, spawns one task per connection via `spawn_connection` (Binance futures needs two), each reconnecting on its own (immediate on clean close, exponential backoff 1s→30s on error; ERROR log only after 3 consecutive failures), spawns the writer, and on SIGTERM/Ctrl-C aborts all collector tasks so the writer drains and exits.

- `config.rs` — `ASSETS` / `MARKETS` (Kraken) / `BINANCE_MARKETS` env vars → the enabled `Market`s and Kraken instrument lists. `BINANCE_MARKETS` is `spot,futures` by default; any value without those words (e.g. `none`) disables Binance. Asset `btc` maps to spot `BTC/USD` (dir `btc_usd`) and futures `PF_XBTUSD` (dir `pf_xbtusd`); others to `<ASSET>/USD` and `PF_<ASSET>USD`.

- `kraken_spot.rs` — `wss://ws.kraken.com/v2`; subscribes `book` (depth 10) and `trade` (no snapshot). Maintains a local `Book` per symbol from snapshot + updates and verifies Kraken's CRC32 checksum after every message. Heartbeats arrive automatically (~1/s).

- `kraken_futures.rs` — `wss://futures.kraken.com/ws/v1`; subscribes `book`, `trade`, `ticker`, `heartbeat`. Book deltas carry one level each and a per-product `seq` that continues the snapshot's seq by +1. Sends a WS ping every 30s (Kraken requires one at least every 60s). `trade_snapshot` is ignored to avoid duplicate rows across reconnects.

  Both Kraken connections: a 100ms sampler emits a `depth10_tick` row for every book that changed since the last sample; if no frame arrives for 10s, or the book desyncs (spot checksum mismatch / futures seq gap → `book::Desync`), `run()` returns `Err` and the connection is rebuilt from a fresh snapshot.

- `binance.rs` — BTCUSDT over combined-stream URLs (`{"stream": …, "data": …}`, dispatched on `stream`). Three sockets (`Feed`): spot `wss://stream.binance.com:9443/stream` (`aggTrade`, `depth5@100ms`); futures `wss://fstream.binance.com/public/stream` (`depth10@100ms`) and `…/market/stream` (`aggTrade`, `markPrice@1s`, `forceOrder`). Binance serves futures book streams only on `/public` and the rest only on `/market` (the legacy `/stream` route now delivers just the book), hence two futures sockets. The exchange's partial-depth snapshots are written as-is (no local book). Pings are answered with pongs; the 24h server close reconnects quietly. A per-socket watchdog fails the connection after 10s without its heartbeat stream (spot depth, futures depth, markPrice — never `forceOrder`/`aggTrade`, which can be quiet). A 403/451 handshake refusal (geo-block) is logged at ERROR every attempt. `BINANCE_SPOT_URL` / `BINANCE_FUTURES_PUBLIC_URL` / `BINANCE_FUTURES_MARKET_URL` override the endpoints for testing (same streams required). Fixtures: `src/testdata/binance_streams.jsonl`.

- `book.rs` — the local L2 book (`BTreeMap<Decimal, Decimal>` per side). Prices/quantities are kept as exact `Decimal`s with their wire scale (decoded from the raw JSON number text via `serde_json::value::RawValue`) because the spot checksum depends on trailing zeros. Test fixture: `src/testdata/kraken_spot_book_btcusd.jsonl`.

- `writer.rs` — buffers rows per `(market, instrument, stream)` and flushes either every 5 seconds or when a buffer reaches 500 rows, to `data/<venue>/<market>/<instrument>/<stream>_YYYYMMDD.csv` (`Market::dir()`; appended across restarts; header written only for new files). Every 5s it logs a `"flush tick"` event (`total_depth`, `total_trades`) and one `"market tick"` event per market (`market` = `kraken_spot` / `kraken_futures` / `binance_spot` / `binance_futures`; cumulative `depth`, `trades`, `ticker`, `mark_price`, `liquidations` counters) — **a contract consumed by `logwatch`**.

- `models.rs` — Kraken wire types (`SpotHead`/`SpotMsg`, `FuturesHead`, book/trade/ticker payloads), Binance wire types (`BinanceEnvelope`, decimal strings via `StrNum`), the CSV row types (`DepthTick`, `BinanceSpotDepthTick`, `BinanceFuturesDepthTick` with manual `Serialize` for their level columns; `SpotTradeTick`, `FuturesTradeTick`, `TickerTick`, `BinanceTradeTick`, `MarkPriceTick`, `LiquidationTick`) and the channel type `Record { market, instrument, row }`. Messages are dispatched by first parsing a small "head" struct, then the concrete type (no internally-tagged serde enums: they buffer content and break `RawValue`).

- `bin/uploader.rs` — a **separate binary** (own Docker container) that syncs CSVs under `DATA_DIR` (recursively) to an S3-compatible bucket (used with Hetzner Object Storage) and prunes old local copies. Object key = `<S3_PREFIX>/<path relative to DATA_DIR>`. Every `UPLOAD_INTERVAL_SECS` (default 300) it scans and, for each file, parses the `YYYYMMDD` embedded in the name (`..._YYYYMMDD.csv`):
  - **Upload (finalized only):** files whose date is before the current UTC day are uploaded once via `PutObject`. The current day's (or any future-dated) file is still being appended by the collector and is skipped. A `HeadObject` check means a file already present in the bucket with the same size is not re-uploaded (so restarts don't re-send). Confirmed-present paths are cached in an in-memory `HashSet` to skip the round-trip on later passes.
  - **Retention (delete):** if `RETENTION_DAYS > 0`, finalized files older than that many days are deleted locally — but only after the upload/`HeadObject` step confirmed the object exists in the bucket, so unuploaded data is never destroyed.

  Configured entirely via env vars (`S3_BUCKET` required; `S3_ENDPOINT_URL`, `S3_REGION`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`, `DATA_DIR`, `UPLOAD_INTERVAL_SECS`, `RETENTION_DAYS`); credentials use the standard AWS chain. See `.env.example`. The uploader mounts `./data` read-write (it deletes) and is independent of the collector's lifecycle.

- `bin/logwatch.rs` — observability sidecar reading container logs via the Docker socket; Telegram alerts on WARN/ERROR lines, a missing `"flush tick"`, a frozen `total_depth`, a frozen per-market counter (`kraken_spot.depth`, `kraken_futures.depth`, `kraken_futures.ticker`, `binance_spot.depth`, `binance_futures.depth`, `binance_futures.mark_price` — trades and liquidations are never watched), or a missing uploader `"sync pass complete"`.

## Output schema

Layout: `data/<venue>/{spot,futures}/<instrument>/<stream>_YYYYMMDD.csv`, e.g. `data/kraken/spot/btc_usd/`, `data/kraken/futures/pf_xbtusd/`, `data/binance/futures/btcusdt/`. `recv_ts_us` (local receive time, µs) is on every row and shares one clock across venues.

### Kraken

`depth10_tick` (spot + futures): `recv_ts_us`, `exch_ts_us`, `bid{1-10}_p`, `bid{1-10}_q`, `ask{1-10}_p`, `ask{1-10}_q` — the local book's top 10 levels, sampled every 100ms when it changed. `exch_ts_us` is the exchange timestamp of the last update applied before the sample (futures timestamps have ms resolution). Missing levels are 0.

`trades_tick` (spot): `recv_ts_us`, `exch_ts_us`, `trade_id`, `price`, `qty`, `side`, `ord_type` (`limit`/`market`)

`trades_tick` (futures): `recv_ts_us`, `exch_ts_ms`, `seq`, `uid`, `price`, `qty`, `side`, `trade_type` (`fill`/`liquidation`/`termination`/`block` — liquidations are the `liquidation` rows). Futures `qty` is in base units (BTC for PF_XBTUSD).

`ticker` (futures, several rows/s): `recv_ts_us`, `exch_ts_ms`, `bid`, `ask`, `bid_size`, `ask_size`, `last`, `mark_price`, `index_price`, `premium`, `funding_rate` (absolute), `funding_rate_prediction`, `relative_funding_rate` (fraction), `relative_funding_rate_prediction`, `next_funding_ts_ms` (absolute epoch ms), `open_interest`, `volume_24h`

`side` is the **taker** side (`buy`/`sell`). For comparison with the legacy Binance files: `side=sell` ≙ `is_buyer_maker=true`.

### Binance (BTCUSDT)

Prices and quantities arrive as decimal strings and are written as numbers. Gaps (e.g. across reconnects) are not backfilled; detect them from `agg_trade_id` jumps.

`trades_tick` (spot + futures, from aggTrade): `recv_ts_us`, `event_ts_ms`, `trade_ts_ms`, `agg_trade_id`, `price`, `qty`, `is_buyer_maker`. Spot files are identical in format to the legacy `btcusdt/` files.

`depth5_tick` (spot, partial book every 100ms): `recv_ts_us`, `last_update_id`, `bid1_p`, `bid1_q` … `bid5_q`, `ask1_p` … `ask5_q` — identical to the legacy files.

`depth10_tick` (futures, partial book every 100ms): `recv_ts_us`, `event_ts_ms`, `transaction_ts_ms`, `last_update_id`, `bid1_p`, `bid1_q` … `bid10_q`, `ask1_p` … `ask10_q`. Missing levels are 0.

`mark_price` (futures, 1/s): `recv_ts_us`, `event_ts_ms`, `mark_price`, `index_price`, `est_settle_price`, `funding_rate`, `next_funding_ts_ms`

`liquidations` (futures, from forceOrder): `recv_ts_us`, `event_ts_ms`, `trade_ts_ms`, `side`, `order_type`, `time_in_force`, `orig_qty`, `price`, `avg_price`, `status`, `last_filled_qty`, `filled_acc_qty`. `side` is the side of the liquidation order: `SELL` = a long was liquidated, `BUY` = a short.

**Limitation:** `forceOrder` is a *snapshot* stream — Binance pushes at most the latest liquidation per symbol per second, so during cascades most liquidations are missing. The file shows *that* liquidations happen and their rough intensity, not total liquidated volume.
