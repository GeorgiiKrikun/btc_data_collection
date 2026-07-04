# Plan: structured (JSON) logging + robust logwatch parsing

## Goal

Stop scraping human-formatted, ANSI-colored text to recover machine facts (log
level, the depth counter). Switch the collector and uploader to **JSON logs**,
promote the heartbeat counters to **structured fields**, and rewrite
`logwatch`'s parser to read JSON instead of substring/space matching.

This keeps the current architecture (an external sidecar that reads container
logs over the Docker socket) — it only makes the pipe reliable.

## Motivation

- `logwatch` currently detects levels via `line.contains(" ERROR ")` and reads
  the counter via `parse_u64_after(line, "total depth=")`. Both are brittle: the
  ANSI-color bug already broke level detection once (see `strip_ansi` in
  `src/bin/logwatch.rs`), and any change to a log message would silently break
  parsing.
- Logs are formatted for humans; JSON gives machines a stable contract:
  `level` is a field, `total_depth` is a number — no ANSI, no alignment spaces,
  no ambiguity.

Non-goal (deferred): a full metrics stack (Prometheus `/metrics` +
Alertmanager). That is the "proper" long-term home for the *data-flow* signals,
but it is more infrastructure than this single collector needs today. Revisit if
we want dashboards / historical rates. This plan is the low-effort, high-value
step that does not commit us to that.

---

## Changes

### 1. Enable the JSON feature — `Cargo.toml`

```toml
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
```

### 2. Collector subscriber → JSON — `src/main.rs` (~lines 16–21)

```rust
tracing_subscriber::fmt()
    .json()
    .with_env_filter(
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "quant_2=info".into()),
    )
    .init();
```

### 3. Uploader subscriber → JSON — `src/bin/uploader.rs`

Same one-line `.json()` addition on its `tracing_subscriber::fmt()` builder.

### 4. Promote the heartbeat counters to fields — `src/writer.rs:32`

Replace the interpolated message with structured fields (message becomes a
stable constant):

```rust
info!(
    depth_buf = depth_buf.len(),
    trade_buf = trade_buf.len(),
    total_depth,
    total_trades,
    "flush tick"
);
```

Emitted JSON (fields nested under `fields` by default):

```json
{"timestamp":"…","level":"INFO","target":"quant_2::writer",
 "fields":{"message":"flush tick","total_depth":1389,"total_trades":1242,
           "depth_buf":50,"trade_buf":56}}
```

### 5. Structure the uploader heartbeat — `src/bin/uploader.rs:247`

```rust
info!(uploaded, deleted, in_progress, "sync pass complete");
```

### 6. Rewrite `logwatch` parsing — `src/bin/logwatch.rs`

Replace the text-scraping in `handle_line` with JSON parsing:

```rust
fn handle_line(line: &str, kind: Kind, service: &str, state: &Mutex<State>, alerter: &Alerter) {
    // Lines that aren't JSON (panics, startup banners, dependency logs) are ignored.
    let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { return };

    let level = v["level"].as_str().unwrap_or("");
    let msg = v["fields"]["message"].as_str().unwrap_or("");

    match level {
        "ERROR" => alerter.transient(&format!("{service}:error"), format!("🔴 [{service}] {msg}")),
        "WARN"  => alerter.transient(&format!("{service}:warn"),  format!("🟠 [{service}] {msg}")),
        _ => {}
    }

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
        }
        Kind::Uploader => {
            if msg == "sync pass complete" {
                st.last_sync_pass = Some(now);
                debug!("uploader heartbeat: 'sync pass complete' seen");
            }
        }
    }
}
```

Then **delete the now-dead helpers**: `strip_ansi`, `level_of`, `parse_u64_after`,
and the `Level` enum. Remove the `strip_ansi(...)` call in `stream_logs` (feed
the trimmed line straight to `handle_line`). Keep the line-buffer splitting on
`\n` — JSON logs are still newline-delimited (one object per line).

> Note: if `.json()` is configured with `.flatten_event(true)`, fields sit at the
> top level (`v["total_depth"]`, `v["message"]`) instead of under `fields`.
> We default to **nested** (no `flatten_event`), so use `v["fields"][...]` as above.

---

## Rollout

All three services share one image (`btc_data_collection:latest`), so the JSON
**producers** (collector, uploader) and the JSON **consumer** (logwatch) always
ship together — there is no mixed old-text/new-JSON version skew as long as we
rebuild and restart them together:

```bash
docker compose up -d --build
```

The 60s `STARTUP_GRACE_SECS` in logwatch covers the brief restart window.

## Verification

1. `LOGWATCH_LOG=logwatch=trace` in `.env`, `docker compose up -d --build`.
2. Confirm the raw lines logwatch ingests are now JSON objects:
   `trace: [quant2] {"timestamp":...,"level":"INFO",...,"total_depth":...}`.
3. Confirm heartbeat parsing still fires:
   `debug: collector heartbeat: total_depth=… advanced=true`
   `debug: uploader heartbeat: 'sync pass complete' seen`.
4. End-to-end alert test: stop the collector → `🔴 no 'Flush tick' …` within
   ~30–40s; restart → `🟢 heartbeat resumed`.
5. Error-forwarding test (previously broken by ANSI): trigger/observe a real
   `WARN`/`ERROR` (e.g. a WebSocket drop) and confirm a clean, un-garbled
   Telegram message.
6. Set `LOGWATCH_LOG=logwatch=info` again once satisfied.

## Risks / notes

- **Dozzle readability:** *not a concern* — Dozzle has automatic JSON detection
  and color-codes rows by the `level` field, so structured logs render **better**
  in the Dozzle UI than the current pretty text, not worse. (Verified in the
  Dozzle docs: "What is Dozzle?" / Alerts & Webhooks.) JSON is Dozzle's preferred
  format and is what its SQL Engine and alerting operate on.
- **Third-party / panic lines** won't be JSON; `handle_line` ignores non-JSON
  lines by design. Panics still surface in Dozzle and (because the container
  exits) as a `no 'Flush tick'` staleness alert.
- **Field name stability** is now the contract: renaming `total_depth` or the
  `"sync pass complete"` message is a breaking change for logwatch — keep them
  in sync (add a comment at each log site pointing at logwatch).

---

## Follow-on (optional): let Dozzle own error alerts, shrink logwatch

Once logs are JSON (above), **Dozzle v10's built-in alerting** can natively
handle the "loud error" half of logwatch. The guiding split:

> **Dozzle alerts on *presence*** — an incoming log line matched a condition.
> **logwatch alerts on *absence*** — an expected line/value did not appear or
> stopped changing. Dozzle structurally cannot do absence (its alerts are
> triggered *by* a line), so that stays logwatch's job.

### Division of responsibilities

| Signal | Owner after this change |
|---|---|
| `WARN`/`ERROR` log lines | **Dozzle** — `Log: level == "error"` (needs JSON) |
| Container exited (crash/OOM/stop) | **Dozzle** — lifecycle-event alert |
| Counter frozen (alive but no Binance data) | **logwatch** — absence of change, unique |
| Heartbeat silence (process hung, container up) | **logwatch** — absence of `flush tick` |
| Uploader stalled (no `sync pass complete`) | **logwatch** — absence, unique |
| Host / Docker / Dozzle itself dead | **healthchecks.io** deadman (logwatch pings it) |

### Dozzle setup

- Already on `amir20/dozzle:latest` (v10+), so no image change.
- **Add a `/data` volume** to the `dozzle` service in `docker-compose.yml` — alert
  and destination config is stored there and must persist across restarts:
  ```yaml
  dozzle:
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./dozzle/data:/data          # <-- add: persists alert config
  ```
- Configure a Log alert in the Dozzle UI, e.g. `Log: level == "error"` (optionally
  scoped with `Container: labels["com.docker.compose.service"] in [...]`).
- Destination: Dozzle has built-in **Slack / Discord / ntfy / custom webhook**,
  but **no native Telegram**. To keep alerts in the existing Telegram chat, use a
  **custom webhook** with a Go-template body POSTing to
  `https://api.telegram.org/bot<token>/sendMessage`. Otherwise ntfy/Slack/Discord
  are one-click.

### logwatch shrink

Remove the error-forwarding path from `handle_line` (the `level`-based
`transient` alerts and the `{service}:error` / `{service}:warn` keys). What
remains is a focused **silence/staleness watcher**:

- collector heartbeat absence (`last_flush_tick`)
- counter freeze (`last_depth` / `last_depth_change`)
- uploader sync-pass absence (`last_sync_pass`)
- the healthchecks.io deadman ping

`handle_line` then only needs `fields.total_depth` (collector) and
`message == "sync pass complete"` (uploader); the `level` read can go too.

### Trade-off

This splits alerting across two systems (Dozzle UI + logwatch env) — each doing
what it is best at, but two places to configure. If you prefer a **single
Telegram integration point**, keep error-forwarding in logwatch as designed and
simply enjoy Dozzle's nicer JSON rendering without wiring its alerts. Both are
valid; this section is opt-in.

---

## Checklist

### Core (JSON migration)

- [ ] `Cargo.toml`: add `json` feature to `tracing-subscriber`
- [ ] `src/main.rs`: `.json()` on the subscriber
- [ ] `src/bin/uploader.rs`: `.json()` on the subscriber
- [ ] `src/writer.rs:32`: structured `flush tick` fields
- [ ] `src/bin/uploader.rs:247`: structured `sync pass complete` fields
- [ ] `src/bin/logwatch.rs`: JSON `handle_line`; delete `strip_ansi`/`level_of`/`parse_u64_after`/`Level`
- [ ] `cargo build --release`
- [ ] Deploy (`docker compose up -d --build`) and run verification steps

### Optional follow-on (Dozzle-native error alerts)

- [ ] `docker-compose.yml`: add `./dozzle/data:/data` volume to the `dozzle` service
- [ ] Dozzle UI: create Log alert `level == "error"` (+ container scope if desired)
- [ ] Dozzle UI: add destination (ntfy/Slack/Discord, or custom webhook → Telegram)
- [ ] `src/bin/logwatch.rs`: drop the `level`-based error-forwarding from `handle_line`
- [ ] Verify Dozzle alert fires on a real error; confirm no duplicate from logwatch
