//! Uploads the date-partitioned CSV files in the data directory to an
//! S3-compatible object store (AWS S3, Hetzner Object Storage, MinIO, ...)
//! and prunes old local copies once they are safely in the bucket.
//!
//! Runs as a standalone service. Every `UPLOAD_INTERVAL_SECS` it scans the
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
//!   S3_PREFIX            (optional)  key prefix, e.g. "btcusdt" -> btcusdt/<file>.csv
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

fn object_key(prefix: &str, file_name: &str) -> String {
    if prefix.is_empty() {
        file_name.to_string()
    } else {
        format!("{prefix}/{file_name}")
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

/// One sync pass. `confirmed` holds paths we know are present in the bucket, so
/// repeated passes skip the HeadObject/PutObject round-trips for them.
async fn sync_once(client: &Client, cfg: &Config, confirmed: &mut HashSet<PathBuf>) {
    let today = Utc::now().date_naive();

    let mut dir = match tokio::fs::read_dir(&cfg.data_dir).await {
        Ok(d) => d,
        Err(e) => {
            error!("cannot read data dir {}: {e}", cfg.data_dir);
            return;
        }
    };

    let mut uploaded = 0u32;
    let mut deleted = 0u32;
    let mut in_progress = 0u32;

    loop {
        let entry = match dir.next_entry().await {
            Ok(Some(e)) => e,
            Ok(None) => break,
            Err(e) => {
                error!("error iterating data dir: {e}");
                break;
            }
        };

        let path = entry.path();
        let file_name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) if n.ends_with(".csv") => n.to_string(),
            _ => continue,
        };

        let Some(file_date) = parse_file_date(&file_name) else {
            warn!("skipping {file_name}: no parseable date in name");
            continue;
        };

        // Only act on finalized files; today's (and any future-dated) file is
        // still being written.
        if file_date >= today {
            in_progress += 1;
            continue;
        }

        let meta = match entry.metadata().await {
            Ok(m) if m.is_file() => m,
            Ok(_) => continue,
            Err(e) => {
                warn!("stat {file_name}: {e}");
                continue;
            }
        };
        let len = meta.len();
        let key = object_key(&cfg.prefix, &file_name);

        // Ensure the file is in the bucket (upload once).
        if !confirmed.contains(&path) {
            let present = match object_present(client, &cfg.bucket, &key, len).await {
                Ok(p) => p,
                Err(e) => {
                    error!("head {file_name} failed: {e:#}");
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
                        info!("uploaded {file_name} ({len} bytes) -> s3://{}/{key}", cfg.bucket);
                    }
                    Err(e) => {
                        error!("upload {file_name} failed: {e:#}");
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
                        info!("deleted local {file_name} (age {age}d > {}d)", cfg.retention_days);
                    }
                    Err(e) => warn!("delete {file_name} failed: {e}"),
                }
            }
        }
    }

    info!(
        "sync pass complete: {uploaded} uploaded, {deleted} deleted, {in_progress} in-progress (skipped)"
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
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
