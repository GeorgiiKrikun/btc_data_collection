//! Uploads the date-partitioned CSV files in the data directory to an
//! S3-compatible object store (AWS S3, Hetzner Object Storage, MinIO, ...)
//! and prunes old local copies once they are safely in the bucket.
//!
//! Runs as a standalone service. Every `UPLOAD_INTERVAL_SECS` it recursively scans the
//! data directory and:
//!   1. Uploads only *finalized* files — those whose embedded date
//!      (`..._YYYYMMDD.csv`) is before the current UTC day. The current day's
//!      CSV is still being appended to by the collector, so it is left alone
//!      until the day rolls over. A finalized file is uploaded once (skipped
//!      thereafter if already present in the bucket with the same size).
//!   2. If `RETENTION_DAYS > 0`, deletes local finalized files older than that
//!      many days — but only after confirming (via HeadObject) the object
//!      exists in the bucket, so unuploaded data is never destroyed.
//!
//! Configuration (environment variables):
//!   S3_BUCKET            (required)  target bucket name
//!   S3_ENDPOINT_URL      (optional)  e.g. https://fsn1.your-objectstorage.com for Hetzner
//!   S3_REGION            (optional)  region string; defaults to AWS_REGION or "us-east-1"
//!   S3_PREFIX            (optional)  key prefix; keys are <prefix>/<path relative to DATA_DIR>,
//!                                    e.g. kraken/spot/btc_usd/trades_tick_20261027.csv
//!   S3_FORCE_PATH_STYLE  (optional)  "true" to use path-style addressing
//!   DATA_DIR             (optional)  directory to upload, default "data"
//!   UPLOAD_INTERVAL_SECS (optional)  scan interval, default 300
//!   RETENTION_DAYS       (optional)  delete local files older than N days; 0/unset disables
//! Credentials use the standard AWS chain (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use chrono::{NaiveDate, Utc};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::time::{interval, Duration};
use tracing::{error, info, warn};

struct Config {
    bucket: String,
    prefix: String,
    endpoint_url: Option<String>,
    region: String,
    force_path_style: bool,
    data_dir: String,
    interval_secs: u64,
    retention_days: i64,
}

fn load_config() -> Result<Config> {
    let bucket = std::env::var("S3_BUCKET").context("S3_BUCKET must be set")?;
    let prefix = std::env::var("S3_PREFIX")
        .unwrap_or_default()
        .trim_matches('/')
        .to_string();
    let endpoint_url = std::env::var("S3_ENDPOINT_URL").ok().filter(|s| !s.is_empty());
    let region = std::env::var("S3_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string());
    let force_path_style = std::env::var("S3_FORCE_PATH_STYLE")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let interval_secs = std::env::var("UPLOAD_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let retention_days = std::env::var("RETENTION_DAYS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    Ok(Config {
        bucket,
        prefix,
        endpoint_url,
        region,
        force_path_style,
        data_dir,
        interval_secs,
        retention_days,
    })
}

async fn build_client(cfg: &Config) -> Client {
    let region = aws_sdk_s3::config::Region::new(cfg.region.clone());
    let shared = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(region)
        .load()
        .await;

    let mut s3_cfg = aws_sdk_s3::config::Builder::from(&shared);
    if let Some(endpoint) = &cfg.endpoint_url {
        s3_cfg = s3_cfg.endpoint_url(endpoint);
    }
    if cfg.force_path_style {
        s3_cfg = s3_cfg.force_path_style(true);
    }
    Client::from_conf(s3_cfg.build())
}

fn object_key(prefix: &str, rel_path: &str) -> String {
    if prefix.is_empty() {
        rel_path.to_string()
    } else {
        format!("{prefix}/{rel_path}")
    }
}

/// Parse the `YYYYMMDD` date embedded in a `..._YYYYMMDD.csv` file name.
fn parse_file_date(file_name: &str) -> Option<NaiveDate> {
    let stem = file_name.strip_suffix(".csv")?;
    let token = stem.rsplit('_').next()?;
    if token.len() == 8 && token.bytes().all(|b| b.is_ascii_digit()) {
        NaiveDate::parse_from_str(token, "%Y%m%d").ok()
    } else {
        None
    }
}

/// Whether the object already exists in the bucket with the given size.
async fn object_present(client: &Client, bucket: &str, key: &str, len: u64) -> Result<bool> {
    match client.head_object().bucket(bucket).key(key).send().await {
        Ok(out) => Ok(out.content_length() == Some(len as i64)),
        Err(e) => {
            let svc = e.into_service_error();
            if svc.is_not_found() {
                Ok(false)
            } else {
                Err(anyhow::Error::new(svc).context(format!("HeadObject {key}")))
            }
        }
    }
}

async fn upload_file(client: &Client, bucket: &str, key: &str, path: &Path) -> Result<()> {
    let body = ByteStream::from_path(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .content_type("text/csv")
        .send()
        .await
        .with_context(|| format!("PutObject {key}"))?;
    Ok(())
}

/// Recursively lists the `.csv` files under `root`, each with its path relative
/// to `root` using `/` separators (e.g. `kraken/spot/btc_usd/trades_tick_20261027.csv`).
/// Returns `None` if `root` itself can't be read.
async fn list_csv_files(root: &Path) -> Option<Vec<(PathBuf, String)>> {
    let mut files = Vec::new();
    let mut pending = vec![root.to_path_buf()];
    while let Some(dir_path) = pending.pop() {
        let mut dir = match tokio::fs::read_dir(&dir_path).await {
            Ok(d) => d,
            Err(e) if dir_path == root => {
                error!("cannot read data dir {}: {e}", root.display());
                return None;
            }
            Err(e) => {
                warn!("cannot read {}: {e}", dir_path.display());
                continue;
            }
        };
        loop {
            let entry = match dir.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    error!("error iterating {}: {e}", dir_path.display());
                    break;
                }
            };
            let path = entry.path();
            match entry.file_type().await {
                Ok(t) if t.is_dir() => pending.push(path),
                Ok(t) if t.is_file() && path.extension().is_some_and(|x| x == "csv") => {
                    let Ok(rel) = path.strip_prefix(root) else { continue };
                    let rel = rel
                        .components()
                        .map(|c| c.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/");
                    files.push((path, rel));
                }
                Ok(_) => {}
                Err(e) => warn!("stat {}: {e}", path.display()),
            }
        }
    }
    Some(files)
}

/// One sync pass. `confirmed` holds paths we know are present in the bucket, so
/// repeated passes skip the HeadObject/PutObject round-trips for them.
async fn sync_once(client: &Client, cfg: &Config, confirmed: &mut HashSet<PathBuf>) {
    let today = Utc::now().date_naive();

    let Some(files) = list_csv_files(Path::new(&cfg.data_dir)).await else {
        return;
    };

    let mut uploaded = 0u32;
    let mut deleted = 0u32;
    let mut in_progress = 0u32;

    for (path, rel_path) in files {
        let file_name = rel_path.rsplit('/').next().unwrap_or(&rel_path);

        let Some(file_date) = parse_file_date(file_name) else {
            warn!("skipping {rel_path}: no parseable date in name");
            continue;
        };

        // Only act on finalized files; today's (and any future-dated) file is
        // still being written.
        if file_date >= today {
            in_progress += 1;
            continue;
        }

        let len = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(e) => {
                warn!("stat {rel_path}: {e}");
                continue;
            }
        };
        let key = object_key(&cfg.prefix, &rel_path);

        // Ensure the file is in the bucket (upload once).
        if !confirmed.contains(&path) {
            let present = match object_present(client, &cfg.bucket, &key, len).await {
                Ok(p) => p,
                Err(e) => {
                    error!("head {rel_path} failed: {e:#}");
                    continue;
                }
            };
            if present {
                confirmed.insert(path.clone());
            } else {
                match upload_file(client, &cfg.bucket, &key, &path).await {
                    Ok(()) => {
                        confirmed.insert(path.clone());
                        uploaded += 1;
                        info!(file = %rel_path, bytes = len, bucket = %cfg.bucket, key = %key, "uploaded");
                    }
                    Err(e) => {
                        error!("upload {rel_path} failed: {e:#}");
                        continue;
                    }
                }
            }
        }

        // Retention: prune local copies that are safely in the bucket.
        if cfg.retention_days > 0 {
            let age = (today - file_date).num_days();
            if age > cfg.retention_days && confirmed.contains(&path) {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {
                        confirmed.remove(&path);
                        deleted += 1;
                        info!("deleted local {rel_path} (age {age}d > {}d)", cfg.retention_days);
                    }
                    Err(e) => warn!("delete {rel_path} failed: {e}"),
                }
            }
        }
    }

    // NOTE: the "sync pass complete" message is a contract consumed by the
    // `logwatch` binary — keep it in sync.
    info!(uploaded, deleted, in_progress, "sync pass complete");
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "uploader=info".into()),
        )
        .init();

    let cfg = load_config()?;
    info!(
        "uploader starting: dir={} -> bucket={} prefix={:?} endpoint={:?} region={} interval={}s retention={}d",
        cfg.data_dir,
        cfg.bucket,
        cfg.prefix,
        cfg.endpoint_url,
        cfg.region,
        cfg.interval_secs,
        cfg.retention_days,
    );

    let client = build_client(&cfg).await;
    let mut confirmed: HashSet<PathBuf> = HashSet::new();

    let mut tick = interval(Duration::from_secs(cfg.interval_secs));

    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;

    loop {
        tokio::select! {
            _ = tick.tick() => {
                sync_once(&client, &cfg, &mut confirmed).await;
            }
            _ = tokio::signal::ctrl_c() => {
                info!("shutdown signal received");
                break;
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received");
                break;
            }
        }
    }

    info!("running final sync before exit");
    sync_once(&client, &cfg, &mut confirmed).await;
    Ok(())
}
