# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Build
cargo build --release

# Run locally (data written to ./data/)
cargo run --release

# Control log level (default: info)
RUST_LOG=quant_2=debug cargo run --release

# Run in Docker (production mode, auto-restarts)
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

## Output schema

`depth5_tick_YYYYMMDD.csv`: `recv_ts_us`, `last_update_id`, `bid{1-5}_p`, `bid{1-5}_q`, `ask{1-5}_p`, `ask{1-5}_q`

`trades_tick_YYYYMMDD.csv`: `recv_ts_us`, `event_ts_ms`, `trade_ts_ms`, `agg_trade_id`, `price`, `qty`, `is_buyer_maker`

`is_buyer_maker=true` means the buyer was the maker (taker sell); `false` means taker buy.
