Binance provides several APIs that give you what you need:

WebSocket streams (real-time)
- <symbol>@depth — full order book updates (bids/asks at each price level)
- <symbol>@trade — every individual trade as it happens (price, quantity, whether buyer or seller was maker)
- <symbol>@aggTrade — trades aggregated by price level, lower bandwidth

REST API (historical)
- /api/v3/depth — current order book snapshot
- /api/v3/aggTrades — historical aggregated trades, paginated by time

What you'd extract from this:

From the order book:
- Bid/ask spread
- Order book imbalance: (bid_volume - ask_volume) / (bid_volume + ask_volume) at top N levels — this is a strong short-term predictor
- Wall detection: are there unusually large orders sitting at a nearby price?

From the trade feed:
- Taker buy ratio at finer granularity than 1-minute candles (you already have the 1m version from Binance CSVs, but sub-minute would be richer)
- Trade size distribution — large trades vs small trades arriving

Practical approach for your setup

You'd run a collector that subscribes to the WebSocket streams and writes to a database or flat files, then aggregate features into 1-minute windows aligned with your existing candle data. The simplest starting point is just the order book imbalance — subscribe to btcusdt@depth5 (top 5 levels), snapshot every few seconds, aggregate into per-minute statistics (mean, std, last value).

The main constraint is that this data isn't historically available for free — Binance only gives you the current and near-current order book. For historical order book data you'd need a paid provider (Tardis.dev is the standard one, has full Binance order book history).

For a first pass, I'd start collecting from now and train on fresh data once you have a few weeks of it.
