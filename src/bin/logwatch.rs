//! logwatch — an external observability sidecar for the data collector.
//!
//! It touches neither the collector nor the uploader: it reads their logs
//! through the Docker socket (read-only, exactly like Dozzle) and pushes a
//! Telegram message when something looks wrong. It watches for:
//!
//!   * collector: any WARN/ERROR line (loud failures)
//!   * collector: absence of the every-5s "flush tick" event  -> process dead/hung
//!   * collector: the `total_depth` counter not advancing     -> connected but no data
//!   * collector: a per-market "market tick" counter frozen    -> one Kraken socket
//!                (spot or futures) down while the other still flows
//!   * uploader:  any WARN/ERROR line (S3 down, bad creds, ...)
//!   * uploader:  absence of "sync pass complete" for too long -> uploader stuck/dead
//!
//! Optionally it pings a HEALTHCHECK_URL (healthchecks.io / Better Stack) every
//! 60s so you also get alerted if the whole host or this sidecar dies.
//!
//! Config (env): TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (required); HEALTHCHECK_URL,
//! COLLECTOR_SERVICE (default "quant2"), UPLOADER_SERVICE (default "uploader"),
//! FLUSH_MAX_SILENCE_SECS (30), COUNTER_MAX_STALE_SECS (60),
//! UPLOADER_MAX_SILENCE_SECS (900), ALERT_COOLDOWN_SECS (300),
//! STARTUP_GRACE_SECS (60), CHECK_INTERVAL_SECS (10).

use anyhow::{Context, Result};
use bollard::container::{ListContainersOptions, LogsOptions};
use bollard::Docker;
use futures_util::StreamExt;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, info, trace, warn};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Config {
    bot_token: String,
    chat_id: String,
    healthcheck_url: Option<String>,
    collector_service: String,
    uploader_service: String,
    flush_max_silence: Duration,
    counter_max_stale: Duration,
    uploader_max_silence: Duration,
    cooldown: Duration,
    startup_grace: Duration,
    check_interval: Duration,
    healthy_ping: Duration,
}

fn env_secs(key: &str, default: u64) -> Duration {
    Duration::from_secs(
        std::env::var(key)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default),
    )
}

impl Config {
    fn from_env() -> Result<Self> {
        Ok(Self {
            bot_token: std::env::var("TELEGRAM_BOT_TOKEN")
                .context("TELEGRAM_BOT_TOKEN is required")?,
            chat_id: std::env::var("TELEGRAM_CHAT_ID").context("TELEGRAM_CHAT_ID is required")?,
            healthcheck_url: std::env::var("HEALTHCHECK_URL").ok().filter(|s| !s.is_empty()),
            collector_service: std::env::var("COLLECTOR_SERVICE")
                .unwrap_or_else(|_| "quant2".into()),
            uploader_service: std::env::var("UPLOADER_SERVICE")
                .unwrap_or_else(|_| "uploader".into()),
            flush_max_silence: env_secs("FLUSH_MAX_SILENCE_SECS", 30),
            counter_max_stale: env_secs("COUNTER_MAX_STALE_SECS", 60),
            uploader_max_silence: env_secs("UPLOADER_MAX_SILENCE_SECS", 900),
            cooldown: env_secs("ALERT_COOLDOWN_SECS", 300),
            startup_grace: env_secs("STARTUP_GRACE_SECS", 60),
            check_interval: env_secs("CHECK_INTERVAL_SECS", 10),
            // Periodic "still healthy" confirmation. Default 6h; 0 disables.
            healthy_ping: env_secs("HEALTHY_PING_SECS", 21600),
        })
    }
}

// ---------------------------------------------------------------------------
// Shared state — updated by log tasks, read by the staleness task
// ---------------------------------------------------------------------------

/// Per-market counters from the collector's "market tick" events that must keep
/// advancing. Each one maps to one exchange socket: spot depth (spot WS), futures
/// depth (futures book) and futures ticker (pushed every second per product, so
/// it is never legitimately quiet).
const WATCHED_COUNTERS: &[(&str, &str)] =
    &[("spot", "depth"), ("futures", "depth"), ("futures", "ticker")];

#[derive(Default)]
struct State {
    last_flush_tick: Option<Instant>,
    last_depth: Option<u64>,
    last_depth_change: Option<Instant>,
    last_sync_pass: Option<Instant>,
    /// (market, field) -> (last value, when it last changed)
    counters: HashMap<(String, &'static str), (u64, Instant)>,
}

#[derive(Clone, Copy)]
enum Kind {
    Collector,
    Uploader,
}

// ---------------------------------------------------------------------------
// Alerter — debounced sender in front of the Telegram channel
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Inner {
    last_sent: HashMap<String, Instant>,
    active: HashSet<String>,
}

struct Alerter {
    tx: mpsc::UnboundedSender<String>,
    cooldown: Duration,
    inner: Mutex<Inner>,
}

impl Alerter {
    fn new(tx: mpsc::UnboundedSender<String>, cooldown: Duration) -> Self {
        Self {
            tx,
            cooldown,
            inner: Mutex::new(Inner::default()),
        }
    }

    fn send(&self, msg: String) {
        let _ = self.tx.send(msg);
    }

    /// One-shot event (e.g. an error log line): sent at most once per cooldown.
    fn transient(&self, key: &str, msg: String) {
        let mut i = self.inner.lock().unwrap();
        let now = Instant::now();
        let due = i
            .last_sent
            .get(key)
            .map_or(true, |t| now.duration_since(*t) > self.cooldown);
        if due {
            i.last_sent.insert(key.to_string(), now);
            drop(i);
            debug!("alert fire (transient) key='{key}'");
            self.send(msg);
        } else {
            debug!("alert suppressed (cooldown) key='{key}'");
        }
    }

    /// Level-triggered condition: fires on rising edge, re-reminds after the
    /// cooldown while still active, and sends `recovery` on the falling edge.
    fn sticky(&self, key: &str, active: bool, msg: String, recovery: String) {
        let mut i = self.inner.lock().unwrap();
        let now = Instant::now();
        if active {
            let is_active = i.active.contains(key);
            let due = i
                .last_sent
                .get(key)
                .map_or(true, |t| now.duration_since(*t) > self.cooldown);
            if !is_active || due {
                i.active.insert(key.to_string());
                i.last_sent.insert(key.to_string(), now);
                drop(i);
                debug!("alert fire (sticky, {}) key='{key}'", if is_active { "reminder" } else { "rising edge" });
                self.send(msg);
            }
        } else if i.active.remove(key) {
            i.last_sent.remove(key);
            drop(i);
            debug!("alert clear (sticky, falling edge) key='{key}'");
            self.send(recovery);
        }
    }

    fn has_active(&self) -> bool {
        !self.inner.lock().unwrap().active.is_empty()
    }

    /// Snapshot of the currently-active sticky alert keys (for status summaries).
    fn active_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = self.inner.lock().unwrap().active.iter().cloned().collect();
        keys.sort();
        keys
    }
}

// ---------------------------------------------------------------------------
// Log line parsing
// ---------------------------------------------------------------------------

/// Parses one JSON log line (both services log via `tracing_subscriber::fmt().json()`).
/// The event fields — including the `message` — are nested under `"fields"`.
/// Non-JSON lines (panics, dependency banners) are ignored.
fn handle_line(line: &str, kind: Kind, service: &str, state: &Mutex<State>, alerter: &Alerter) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
        return;
    };
    let level = v["level"].as_str().unwrap_or("");
    let msg = v["fields"]["message"].as_str().unwrap_or("");

    // Loud failures: forward WARN/ERROR straight to Telegram (debounced).
    match level {
        "ERROR" => alerter.transient(&format!("{service}:error"), format!("🔴 [{service}] {msg}")),
        "WARN" => alerter.transient(&format!("{service}:warn"), format!("🟠 [{service}] {msg}")),
        _ => {}
    }

    // Heartbeats: presence of specific structured events (see the log-site
    // comments in writer.rs / uploader.rs — the field/message names are a contract).
    let now = Instant::now();
    let mut st = state.lock().unwrap();
    match kind {
        Kind::Collector => {
            if let Some(d) = v["fields"]["total_depth"].as_u64() {
                st.last_flush_tick = Some(now);
                let advanced = st.last_depth.map_or(true, |prev| d != prev);
                if advanced {
                    st.last_depth = Some(d);
                    st.last_depth_change = Some(now);
                }
                debug!("collector heartbeat: total_depth={d} advanced={advanced}");
            }
            if msg == "market tick" {
                let market = v["fields"]["market"].as_str().unwrap_or("?").to_string();
                for &(m, field) in WATCHED_COUNTERS {
                    if m != market {
                        continue;
                    }
                    let Some(value) = v["fields"][field].as_u64() else { continue };
                    let entry = st.counters.entry((market.clone(), field)).or_insert((value, now));
                    if entry.0 != value {
                        *entry = (value, now);
                    }
                }
            }
        }
        Kind::Uploader => {
            if msg == "sync pass complete" {
                st.last_sync_pass = Some(now);
                debug!("uploader heartbeat: 'sync pass complete' seen");
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Docker log streaming
// ---------------------------------------------------------------------------

/// Lists running containers and matches them to a compose service *client-side*
/// (by the `com.docker.compose.service` label, falling back to container name).
/// We deliberately do NOT use the Docker API's server-side `filters` param: if
/// it matches nothing it returns an empty list with no error, which is silent
/// and hard to debug. Filtering here means we can log exactly what we matched.
async fn list_ids(docker: &Docker, service: &str) -> Result<Vec<(String, String)>> {
    let opts = ListContainersOptions::<String> {
        all: false,
        ..Default::default()
    };
    let containers = docker.list_containers(Some(opts)).await?;
    debug!(
        "service='{service}': {} running container(s) to consider",
        containers.len()
    );
    let mut matched = Vec::new();
    for c in containers {
        let Some(id) = c.id else { continue };
        let name = c
            .names
            .unwrap_or_default()
            .join(",")
            .trim_start_matches('/')
            .to_string();
        let svc = c
            .labels
            .as_ref()
            .and_then(|l| l.get("com.docker.compose.service"))
            .map(String::as_str);
        let is_match = svc == Some(service) || name.contains(service);
        debug!(
            "  candidate name='{name}' compose-service={svc:?} => {}",
            if is_match { "MATCH" } else { "skip" }
        );
        if is_match {
            matched.push((id, name));
        }
    }
    Ok(matched)
}

async fn stream_logs(
    docker: Docker,
    id: String,
    kind: Kind,
    service: String,
    state: Arc<Mutex<State>>,
    alerter: Arc<Alerter>,
) {
    let opts = LogsOptions::<String> {
        follow: true,
        stdout: true,
        stderr: true,
        tail: "0".to_string(),
        ..Default::default()
    };
    let short = &id[..id.len().min(12)];
    info!("streaming logs: service={service} id={short}");

    let mut stream = docker.logs(&id, Some(opts));
    let mut buf = String::new();
    let mut first = true;
    while let Some(item) = stream.next().await {
        match item {
            Ok(output) => {
                buf.push_str(&String::from_utf8_lossy(&output.into_bytes()));
                while let Some(pos) = buf.find('\n') {
                    let line: String = buf.drain(..=pos).collect();
                    let line = line.trim();
                    if !line.is_empty() {
                        if first {
                            debug!("first log line received from service='{service}': {line}");
                            first = false;
                        }
                        trace!("[{service}] {line}");
                        handle_line(line, kind, &service, &state, &alerter);
                    }
                }
            }
            Err(e) => {
                warn!("log stream error for {service} ({short}): {e}");
                break;
            }
        }
    }
    info!("log stream closed: service={service} id={short}");
}

/// Discovers containers for a compose service and keeps one streaming task per
/// container id. A recreated container has a new id, which the next poll picks
/// up. A `docker compose restart` keeps the id but ends the log stream, so an id
/// is forgotten when its stream closes and re-attached on the next poll.
async fn watch_service(
    docker: Docker,
    service: String,
    kind: Kind,
    state: Arc<Mutex<State>>,
    alerter: Arc<Alerter>,
) {
    let tracked: Arc<Mutex<HashSet<String>>> = Arc::default();
    let mut warned_empty = false;
    loop {
        match list_ids(&docker, &service).await {
            Ok(found) => {
                if found.is_empty() {
                    if !warned_empty {
                        warn!(
                            "no running container matched service='{service}' — check COLLECTOR_SERVICE/UPLOADER_SERVICE against the container name/label"
                        );
                        warned_empty = true;
                    }
                } else {
                    warned_empty = false;
                    for (id, name) in found {
                        if tracked.lock().unwrap().insert(id.clone()) {
                            info!("watching container '{name}' for service='{service}'");
                            let stream = stream_logs(
                                docker.clone(),
                                id.clone(),
                                kind,
                                service.clone(),
                                state.clone(),
                                alerter.clone(),
                            );
                            let tracked = tracked.clone();
                            tokio::spawn(async move {
                                stream.await;
                                tracked.lock().unwrap().remove(&id);
                            });
                        }
                    }
                }
            }
            Err(e) => warn!("list containers for {service}: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

// ---------------------------------------------------------------------------
// Staleness detection
// ---------------------------------------------------------------------------

async fn staleness_task(cfg: Arc<Config>, state: Arc<Mutex<State>>, alerter: Arc<Alerter>) {
    let start = Instant::now();
    let mut ticker = interval(cfg.check_interval);
    loop {
        ticker.tick().await;
        let now = Instant::now();
        if now.duration_since(start) < cfg.startup_grace {
            continue;
        }

        let (last_tick, last_change, last_pass, counters) = {
            let st = state.lock().unwrap();
            (
                st.last_flush_tick.unwrap_or(start),
                st.last_depth_change.unwrap_or(start),
                st.last_sync_pass.unwrap_or(start),
                st.counters.clone(),
            )
        };

        // Collector alive? (the every-5s Flush tick is the heartbeat)
        let tick_age = now.duration_since(last_tick);
        let collector_alive = tick_age <= cfg.flush_max_silence;
        debug!(
            "staleness check: flush_tick_age={}s (alive={collector_alive}), depth_change_age={}s, sync_pass_age={}s",
            tick_age.as_secs(),
            now.duration_since(last_change).as_secs(),
            now.duration_since(last_pass).as_secs(),
        );
        alerter.sticky(
            "collector:heartbeat",
            !collector_alive,
            format!(
                "🔴 [collector] no 'Flush tick' for {}s — process dead or hung",
                tick_age.as_secs()
            ),
            "🟢 [collector] heartbeat resumed".into(),
        );

        // Data actually flowing? Only meaningful while the collector is alive.
        if collector_alive {
            let stale = now.duration_since(last_change) > cfg.counter_max_stale;
            alerter.sticky(
                "collector:counter",
                stale,
                format!(
                    "🔴 [collector] depth counter frozen for {}s — connected but NO data from Kraken",
                    now.duration_since(last_change).as_secs()
                ),
                "🟢 [collector] data flow resumed".into(),
            );

            // Per-socket flow: one market can die while the other keeps the
            // total counter moving. Markets the collector doesn't run never
            // appear in `counters` and are not checked.
            for ((market, field), (_, changed)) in &counters {
                let age = now.duration_since(*changed);
                alerter.sticky(
                    &format!("collector:{market}:{field}"),
                    age > cfg.counter_max_stale,
                    format!(
                        "🔴 [collector] {market} {field} counter frozen for {}s — that Kraken feed is down",
                        age.as_secs()
                    ),
                    format!("🟢 [collector] {market} {field} flow resumed"),
                );
            }
        }

        // Uploader still completing sync passes?
        let pass_age = now.duration_since(last_pass);
        alerter.sticky(
            "uploader:sync",
            pass_age > cfg.uploader_max_silence,
            format!(
                "🔴 [uploader] no 'sync pass complete' for {}s — uploader stuck or dead",
                pass_age.as_secs()
            ),
            "🟢 [uploader] sync resumed".into(),
        );
    }
}

// ---------------------------------------------------------------------------
// Periodic "still healthy" confirmation
// ---------------------------------------------------------------------------

/// Sends a positive status summary every `period` so silence can be trusted as
/// "all good" rather than "logwatch is broken". Skips the ping while any alert is
/// active — you're already being told about that — so a green ping is unambiguous.
async fn healthy_ping_task(period: Duration, state: Arc<Mutex<State>>, alerter: Arc<Alerter>) {
    let start = Instant::now();
    let mut ticker = interval(period);
    ticker.tick().await; // consume the immediate first tick — no ping at t=0
    loop {
        ticker.tick().await;
        if alerter.has_active() {
            debug!(
                "healthy ping skipped — active alerts: {:?}",
                alerter.active_keys()
            );
            continue;
        }

        let now = Instant::now();
        let (tick_age, depth, pass_age, mut counters) = {
            let st = state.lock().unwrap();
            (
                st.last_flush_tick.map(|t| now.duration_since(t).as_secs()),
                st.last_depth,
                st.last_sync_pass.map(|t| now.duration_since(t).as_secs()),
                st.counters
                    .iter()
                    .map(|((m, f), (v, _))| format!("{m}.{f}={v}"))
                    .collect::<Vec<_>>(),
            )
        };
        counters.sort();

        let collector = match (tick_age, depth) {
            (Some(a), Some(d)) => format!(
                "collector ok (last tick {a}s ago, depth={d}; {})",
                counters.join(", ")
            ),
            _ => "collector: no data yet".into(),
        };
        let uploader = match pass_age {
            Some(a) => format!("uploader ok (last sync {a}s ago)"),
            None => "uploader: no sync yet".into(),
        };
        let uptime_h = now.duration_since(start).as_secs() / 3600;
        alerter.send(format!(
            "🟢 all healthy — {collector}; {uploader}; logwatch up {uptime_h}h"
        ));
    }
}

// ---------------------------------------------------------------------------
// Outbound: Telegram + healthcheck heartbeat (ureq, ring-backed rustls)
// ---------------------------------------------------------------------------

async fn telegram_task(
    mut rx: mpsc::UnboundedReceiver<String>,
    agent: ureq::Agent,
    token: String,
    chat_id: String,
) {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    while let Some(text) = rx.recv().await {
        let agent = agent.clone();
        let url = url.clone();
        let chat_id = chat_id.clone();
        let res = tokio::task::spawn_blocking(move || {
            agent
                .post(&url)
                .send_json(ureq::json!({ "chat_id": chat_id, "text": text }))
        })
        .await;
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!("telegram send failed: {e}"),
            Err(e) => warn!("telegram task join error: {e}"),
        }
    }
}

/// Dead-man's switch: while logwatch is alive it pings the URL every 60s. It
/// intentionally does NOT report health here — content problems go to Telegram.
/// The healthcheck therefore turns red only when logwatch stops pinging, i.e.
/// the sidecar crashed, the host died, or the network dropped — the one gap
/// Telegram can't cover.
async fn heartbeat_task(agent: ureq::Agent, url: String) {
    let mut ticker = interval(Duration::from_secs(60));
    loop {
        ticker.tick().await;
        let agent = agent.clone();
        let url = url.clone();
        let _ = tokio::task::spawn_blocking(move || agent.get(&url).call()).await;
    }
}

// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "logwatch=info".into()),
        )
        .init();

    let cfg = Arc::new(Config::from_env()?);
    debug!(
        "config: collector='{}' uploader='{}' flush_max={}s counter_max={}s uploader_max={}s cooldown={}s grace={}s check_every={}s healthy_ping={}s healthcheck={} chat_id={}",
        cfg.collector_service,
        cfg.uploader_service,
        cfg.flush_max_silence.as_secs(),
        cfg.counter_max_stale.as_secs(),
        cfg.uploader_max_silence.as_secs(),
        cfg.cooldown.as_secs(),
        cfg.startup_grace.as_secs(),
        cfg.check_interval.as_secs(),
        cfg.healthy_ping.as_secs(),
        cfg.healthcheck_url.is_some(),
        cfg.chat_id,
    );
    let docker = Docker::connect_with_unix_defaults()
        .context("connect to /var/run/docker.sock")?;
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(10))
        .timeout_read(Duration::from_secs(15))
        .build();

    let (tg_tx, tg_rx) = mpsc::unbounded_channel::<String>();
    tokio::spawn(telegram_task(
        tg_rx,
        agent.clone(),
        cfg.bot_token.clone(),
        cfg.chat_id.clone(),
    ));

    let alerter = Arc::new(Alerter::new(tg_tx.clone(), cfg.cooldown));
    let state = Arc::new(Mutex::new(State::default()));

    let _ = tg_tx.send(format!(
        "🟢 logwatch started — watching '{}' + '{}'",
        cfg.collector_service, cfg.uploader_service
    ));
    info!(
        "logwatch started — collector='{}' uploader='{}' healthcheck={}",
        cfg.collector_service,
        cfg.uploader_service,
        cfg.healthcheck_url.is_some()
    );

    tokio::spawn(watch_service(
        docker.clone(),
        cfg.collector_service.clone(),
        Kind::Collector,
        state.clone(),
        alerter.clone(),
    ));
    tokio::spawn(watch_service(
        docker.clone(),
        cfg.uploader_service.clone(),
        Kind::Uploader,
        state.clone(),
        alerter.clone(),
    ));
    tokio::spawn(staleness_task(cfg.clone(), state.clone(), alerter.clone()));
    if let Some(url) = cfg.healthcheck_url.clone() {
        tokio::spawn(heartbeat_task(agent.clone(), url));
    }
    if cfg.healthy_ping > Duration::ZERO {
        tokio::spawn(healthy_ping_task(
            cfg.healthy_ping,
            state.clone(),
            alerter.clone(),
        ));
    }

    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
    info!("logwatch shutting down");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_tick_tracks_only_watched_counters() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let alerter = Alerter::new(tx, Duration::from_secs(300));
        let state = Mutex::new(State::default());
        // A real event as emitted by writer.rs.
        let line = r#"{"timestamp":"2026-10-26T16:56:05.465671Z","level":"INFO","fields":{"message":"market tick","market":"futures","depth":80,"trades":6,"ticker":25},"target":"quant_2::writer"}"#;
        handle_line(line, Kind::Collector, "quant2", &state, &alerter);

        let st = state.lock().unwrap();
        let get = |m: &str, f: &'static str| st.counters.get(&(m.to_string(), f)).map(|c| c.0);
        assert_eq!(get("futures", "depth"), Some(80));
        assert_eq!(get("futures", "ticker"), Some(25));
        assert_eq!(get("futures", "trades"), None, "trades can be legitimately quiet");
        assert_eq!(get("spot", "depth"), None, "spot not seen yet");
    }
}
