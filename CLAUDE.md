# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build (compiles both binaries: quant_2 collector + uploader)
cargo build --release

# Run the collector locally (data written to ./data/)
cargo run --release --bin quant_2

# Run the S3 uploader locally (reads config from env; see .env.example)
S3_BUCKET=... S3_ENDPOINT_URL=... cargo run --release --bin uploader

# Control log level (default: info)
RUST_LOG=quant_2=debug cargo run --release --bin quant_2

# Run in Docker (collector + uploader, production mode, auto-restarts)
cp .env.example .env   # then fill in S3 creds/bucket
UID=$(id -u) GID=$(id -g) docker compose up -d
docker compose logs -f
docker compose down
```

There are no tests in this project.

## Architecture

A Tokio async service that streams BTC/USDT market data from Binance WebSocket and writes it to date-partitioned CSV files in `./data/`.

**Data flow:**

```
Binance WS  →  collector::run()  →  mpsc channels  →  writer::writer_task()  →  CSV files
```

- `main.rs` — installs the rustls crypto provider, sets up two `mpsc::channel` pairs (depth, trades), spawns the collector loop with auto-reconnect (immediate on clean close, 5s delay on error), spawns the writer task, and handles SIGTERM/Ctrl-C by aborting the collector and letting the writer drain.

- `collector.rs` — connects to a Binance combined stream (`btcusdt@depth5@100ms` + `btcusdt@aggTrade`), dispatches messages by stream name, responds to WebSocket pings, and sends typed structs to the channels.

- `writer.rs` — buffers ticks in memory and flushes to CSV either every 5 seconds or when the buffer reaches 500 records. CSV files are date-partitioned (`depth5_tick_YYYYMMDD.csv`, `trades_tick_YYYYMMDD.csv`) and appended to across process restarts; the CSV header is written only if the file doesn't yet exist.

- `models.rs` — two layers of types: raw Binance wire format (`StreamEnvelope`, `DepthData`, `AggTradeData`) and the flattened CSV output types (`DepthTick`, `TradeTick`). `DepthTick` captures all 5 bid/ask price+quantity levels plus a microsecond receive timestamp.

- `bin/uploader.rs` — a **separate binary** (own Docker container) that syncs `./data/*.csv` to an S3-compatible bucket (used with Hetzner Object Storage) and prunes old local copies. Every `UPLOAD_INTERVAL_SECS` (default 300) it scans the data dir and, for each file, parses the `YYYYMMDD` embedded in the name (`..._YYYYMMDD.csv`):
  - **Upload (finalized only):** files whose date is before the current UTC day are uploaded once via `PutObject`. The current day's (or any future-dated) file is still being appended by the collector and is skipped. A `HeadObject` check means a file already present in the bucket with the same size is not re-uploaded (so restarts don't re-send). Confirmed-present paths are cached in an in-memory `HashSet` to skip the round-trip on later passes.
  - **Retention (delete):** if `RETENTION_DAYS > 0`, finalized files older than that many days are deleted locally — but only after the upload/`HeadObject` step confirmed the object exists in the bucket, so unuploaded data is never destroyed.

  Configured entirely via env vars (`S3_BUCKET` required; `S3_ENDPOINT_URL`, `S3_REGION`, `S3_PREFIX`, `S3_FORCE_PATH_STYLE`, `DATA_DIR`, `UPLOAD_INTERVAL_SECS`, `RETENTION_DAYS`); credentials use the standard AWS chain. See `.env.example`. The uploader mounts `./data` read-write (it deletes) and is independent of the collector's lifecycle.

## Output schema

`depth5_tick_YYYYMMDD.csv`: `recv_ts_us`, `last_update_id`, `bid{1-5}_p`, `bid{1-5}_q`, `ask{1-5}_p`, `ask{1-5}_q`

`trades_tick_YYYYMMDD.csv`: `recv_ts_us`, `event_ts_ms`, `trade_ts_ms`, `agg_trade_id`, `price`, `qty`, `is_buyer_maker`

`is_buyer_maker=true` means the buyer was the maker (taker sell); `false` means taker buy.
