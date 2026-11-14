# Plan: compress finished day files (zstd) — not implemented yet

## Why

Raw CSV is about 2.5 GB per day across all streams (2026-10-27: 2.26 GB, 2026-10-28: 2.66 GB). The depth10 files dominate, at 230–285 MB per instrument per day. Measured on `kraken/futures/pf_xbtusd/depth10_tick_20261027.csv` (284 MB):

| codec        | size    | ratio | time (1 core-ish) |
|--------------|---------|-------|-------------------|
| zstd -3      | 28.4 MB | 10.0x | 0.6 s             |
| gzip -6      | 24.2 MB | 11.8x | 4.0 s             |

zstd gives about 10x at negligible CPU. That means about 250 MB/day on local disk and in S3 instead of about 2.5 GB, so `RETENTION_DAYS` can stay at 7+ with room to spare.

## Design

The collector does **not** change, so collection has no risk or downtime: it keeps appending plain CSV to today's files. All the work goes into the uploader, as a *compact* step at the start of each sync pass (`sync_once` in `src/bin/uploader.rs`):

1. For each `*_YYYYMMDD.csv` whose date is before today (UTC) **and** whose mtime is more than 10 minutes old:
   - Stream it through zstd level 3 into `<name>.csv.zst.tmp` in the same directory, inside `spawn_blocking`, using the `zstd` crate's streaming encoder. Memory stays flat.
   - `fsync` the tmp file, rename it to `<name>.csv.zst`, then delete `<name>.csv`.
   - The mtime guard matters: `writer.rs` picks the file date at *flush* time (`Utc::now()`), so a flush that started just before midnight can still be appending to yesterday's file for a moment.
   - If `<name>.csv.zst` already exists (a crash between rename and delete), the `.csv` is the leftover: re-compress over it, which is idempotent, then delete.
   - Delete stray `*.csv.zst.tmp` files (an interrupted compression) on start.
2. Upload, `HeadObject` and retention run on `.csv.zst` exactly as they do on `.csv` today. The object key is the relative path, so it gains the `.zst` suffix. zstd output is deterministic for a given level and library version, but the size check doesn't depend on that: it compares against the local `.zst` file.
3. `parse_file_date` and `list_csv_files` accept both `.csv` and `.csv.zst`. Legacy `.csv` files from before the change are compacted on the first pass.

Upload set `Content-Type: application/zstd` (optional).

## Reading the data

- polars: `pl.read_csv("x.csv.zst")` decompresses transparently. `pl.scan_csv` does not, so decompress first or use `read_csv`.
- pandas: `pd.read_csv("x.csv.zst")` infers `compression="zstd"` from the extension (needs the `zstandard` package).
- shell: `zstdcat x.csv.zst | head`.

## Optional: recompress the bucket

A one-off binary or script that lists `*.csv` objects, downloads, compresses, uploads `.csv.zst`, verifies (size via `HeadObject`, plus a decompress round-trip on a sample), then deletes the original. Run it from a machine with bandwidth, not necessarily the server. It cuts the S3 bill by about 10x for historical data. Keep the legacy `btcusdt/` prefix in scope or not, as you prefer.

## Tests

- Unit: `parse_file_date` handles `.csv.zst`. Compacting a temp dir turns `a_20260101.csv` into `a_20260101.csv.zst`, which round-trips to identical bytes. Today's file and a file with a fresh mtime are left alone.
- Manual: run the uploader against a copy of `data/` with a dummy bucket and check `zstdcat` output against the originals.
