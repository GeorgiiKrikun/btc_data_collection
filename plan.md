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

- **Dozzle readability:** JSON logs are denser to read by eye in the Dozzle web
  UI than the current pretty format. Dozzle does pretty-print detected JSON, so
  this is usually a wash, but worth a look after rollout. If it's annoying, we
  can keep pretty logs for humans *and* emit JSON — but that means two writers;
  simpler to accept JSON everywhere.
- **Third-party / panic lines** won't be JSON; `handle_line` ignores non-JSON
  lines by design. Panics still surface in Dozzle and (because the container
  exits) as a `no 'Flush tick'` staleness alert.
- **Field name stability** is now the contract: renaming `total_depth` or the
  `"sync pass complete"` message is a breaking change for logwatch — keep them
  in sync (add a comment at each log site pointing at logwatch).

## Checklist

- [ ] `Cargo.toml`: add `json` feature to `tracing-subscriber`
- [ ] `src/main.rs`: `.json()` on the subscriber
- [ ] `src/bin/uploader.rs`: `.json()` on the subscriber
- [ ] `src/writer.rs:32`: structured `flush tick` fields
- [ ] `src/bin/uploader.rs:247`: structured `sync pass complete` fields
- [ ] `src/bin/logwatch.rs`: JSON `handle_line`; delete `strip_ansi`/`level_of`/`parse_u64_after`/`Level`
- [ ] `cargo build --release`
- [ ] Deploy (`docker compose up -d --build`) and run verification steps
