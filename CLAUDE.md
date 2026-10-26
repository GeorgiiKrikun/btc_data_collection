# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build (compiles all binaries: quant_2 collector, uploader, logwatch)
cargo build --release

# Unit tests (order-book checksum against a live Kraken capture, logwatch parsing)
cargo test --release

# Run the collector locally (data written to ./data/kraken/ relative to the cwd)
cargo run --release --bin quant_2

# Collect a subset (defaults: ASSETS=btc,eth,sol,xrp,bnb  MARKETS=spot,futures)
ASSETS=btc,eth MARKETS=futures cargo run --release --bin quant_2

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

A Tokio async service that streams market data from **Kraken** (spot + linear perpetual futures) over WebSocket and writes it to date-partitioned CSV files under `./data/kraken/`. (Until Oct 2026 it collected BTCUSDT from Binance; those files remain in the bucket under `btcusdt/`.)

**Data flow:**

```
Kraken spot WS v2     →  kraken_spot::run()     ┐
                                                 ├→  mpsc<Record>  →  writer::writer_task()  →  CSV files
Kraken Futures WS v1  →  kraken_futures::run()  ┘
```

- `main.rs` — installs the rustls crypto provider, reads `config.rs`, creates one `mpsc::channel::<Record>`, spawns one task per enabled market via `spawn_connection` (auto-reconnect: immediate on clean close, 5s delay on error), spawns the writer, and on SIGTERM/Ctrl-C aborts all collector tasks so the writer drains and exits.

- `config.rs` — `ASSETS` / `MARKETS` env vars → instrument lists. Asset `btc` maps to spot `BTC/USD` (dir `btc_usd`) and futures `PF_XBTUSD` (dir `pf_xbtusd`); others to `<ASSET>/USD` and `PF_<ASSET>USD`.

- `kraken_spot.rs` — `wss://ws.kraken.com/v2`; subscribes `book` (depth 10) and `trade` (no snapshot). Maintains a local `Book` per symbol from snapshot + updates and verifies Kraken's CRC32 checksum after every message. Heartbeats arrive automatically (~1/s).

- `kraken_futures.rs` — `wss://futures.kraken.com/ws/v1`; subscribes `book`, `trade`, `ticker`, `heartbeat`. Book deltas carry one level each and a per-product `seq` that continues the snapshot's seq by +1. Sends a WS ping every 30s (Kraken requires one at least every 60s). `trade_snapshot` is ignored to avoid duplicate rows across reconnects.

  Both connections: a 100ms sampler emits a `depth10_tick` row for every book that changed since the last sample; if no frame arrives for 10s, or the book desyncs (spot checksum mismatch / futures seq gap → `book::Desync`), `run()` returns `Err` and the connection is rebuilt from a fresh snapshot.

- `book.rs` — the local L2 book (`BTreeMap<Decimal, Decimal>` per side). Prices/quantities are kept as exact `Decimal`s with their wire scale (decoded from the raw JSON number text via `serde_json::value::RawValue`) because the spot checksum depends on trailing zeros. Test fixture: `src/testdata/kraken_spot_book_btcusd.jsonl`.

- `writer.rs` — buffers rows per `(market, instrument, stream)` and flushes either every 5 seconds or when a buffer reaches 500 rows, to `data/kraken/<market>/<instrument>/<stream>_YYYYMMDD.csv` (appended across restarts; header written only for new files). Every 5s it logs a `"flush tick"` event (`total_depth`, `total_trades`) and one `"market tick"` event per market (`depth`, `trades`, `ticker` cumulative counters) — **a contract consumed by `logwatch`**.

- `models.rs` — Kraken wire types (`SpotHead`/`SpotMsg`, `FuturesHead`, book/trade/ticker payloads), the CSV row types (`DepthTick` with a manual `Serialize` for its 40 level columns, `SpotTradeTick`, `FuturesTradeTick`, `TickerTick`) and the channel type `Record { market, instrument, row }`. Messages are dispatched by first parsing a small "head" struct, then the concrete type (no internally-tagged serde enums: they buffer content and break `RawValue`).

- `bin/uploader.rs` — a **separate binary** (own Docker container) that syncs CSVs under `DATA_DIR` (recursively) to an S3-compatible bucket (used with Hetzner Object Storage) and prunes old local copies. Object key = `<S3_PREFIX>/<path relative to DATA_DIR>`. Every `UPLOAD_INTERVAL_SECS` (default 300) it scans and, for each file, parses the `YYYYMMDD` embedded in the name (`..._YYYYMMDD.csv`):
  - **Upload (finalized only):** files whose date is before the current UTC day are uploaded once via `PutObject`. The current day's (or any future-dated) file is still being appended by the collector and is skipped. A `HeadObject` check means a file already present in the bucket with the same size is not re-uploaded (so restarts don't re-send). Confirmed-present paths are cached in an in-memory `HashSet` to skip the round-trip on later passes.
  - **Retention (delete):** if `RETENTION_DAYS > 0`, finalized files older than that many days are deleted locally — but only after the upload/`HeadObject` step confirmed the object exists in the bucket, so unuploaded data is never destroyed.

  Configured entirely via env vars (`S3_BUCKET` required; `S3_ENDPOINT_URL`, `S3_REGION`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`, `DATA_DIR`, `UPLOAD_INTERVAL_SECS`, `RETENTION_DAYS`); credentials use the standard AWS chain. See `.env.example`. The uploader mounts `./data` read-write (it deletes) and is independent of the collector's lifecycle.

- `bin/logwatch.rs` — observability sidecar reading container logs via the Docker socket; Telegram alerts on WARN/ERROR lines, a missing `"flush tick"`, a frozen `total_depth`, a frozen per-market counter (`spot.depth`, `futures.depth`, `futures.ticker`), or a missing uploader `"sync pass complete"`.

## Output schema

Layout: `data/kraken/{spot,futures}/<instrument>/<stream>_YYYYMMDD.csv`, e.g. `data/kraken/spot/btc_usd/`, `data/kraken/futures/pf_xbtusd/`.

`depth10_tick` (spot + futures): `recv_ts_us`, `exch_ts_us`, `bid{1-10}_p`, `bid{1-10}_q`, `ask{1-10}_p`, `ask{1-10}_q` — the local book's top 10 levels, sampled every 100ms when it changed. `exch_ts_us` is the exchange timestamp of the last update applied before the sample (futures timestamps have ms resolution). Missing levels are 0.

`trades_tick` (spot): `recv_ts_us`, `exch_ts_us`, `trade_id`, `price`, `qty`, `side`, `ord_type` (`limit`/`market`)

`trades_tick` (futures): `recv_ts_us`, `exch_ts_ms`, `seq`, `uid`, `price`, `qty`, `side`, `trade_type` (`fill`/`liquidation`/`termination`/`block` — liquidations are the `liquidation` rows). Futures `qty` is in base units (BTC for PF_XBTUSD).

`ticker` (futures, several rows/s): `recv_ts_us`, `exch_ts_ms`, `bid`, `ask`, `bid_size`, `ask_size`, `last`, `mark_price`, `index_price`, `premium`, `funding_rate` (absolute), `funding_rate_prediction`, `relative_funding_rate` (fraction), `relative_funding_rate_prediction`, `next_funding_ts_ms` (absolute epoch ms), `open_interest`, `volume_24h`

`side` is the **taker** side (`buy`/`sell`). For comparison with the legacy Binance files: `side=sell` ≙ `is_buyer_maker=true`.
