// SPDX-License-Identifier: Apache-2.0
//
// A connection storm against a nats-server WebSocket listener: N connections at once (or spread over STORM_RAMP_MS), each timed from
// CONNECT to the broker's PONG or -ERR ("auth") and from the start of the connection to the PONG ("total").
//
// It is a separate process on purpose: it runs the connections on a pool of worker threads and reports its own scheduling delay (how late the
// runtime wakes a timer that asked for 10 ms), so a limit in the generator is visible instead of being counted as the broker's time.
//
// Authentication: the broker is in operator mode (components/provision, components/callout). Connection i presents its own ticket, a user JWT
// signed by the AUTH account's scoped signing key (STORM_SCOPED_SEED) with the roles STORM_ROLES as tags, and signs the broker's nonce with its
// own user key. The tickets are made before each run's clock starts, so signing a JWT is not part of what is measured; the nonce signature
// cannot be made early (the nonce comes in each connection's INFO) and is made before CONNECT is sent, so it is inside "total" and outside "auth".
//
//   cargo build --release     then run  target/release/auth-storm
//   STORM_URL=wss://broker:8443 STORM_CA_FILE=ca.pem STORM_SCOPED_SEED_FILE=scoped.seed STORM_AUTH_ACCOUNT_FILE=auth.pub \
//     STORM_CONNECTIONS=800 STORM_RUNS=5 target/release/auth-storm
//
// Settings (environment):
//   STORM_URL           the broker's WebSocket URL, ws:// or wss:// (required)
//   STORM_SCOPED_SEED   the scoped signing seed (SA...), or STORM_SCOPED_SEED_FILE, a file holding it (a mounted Secret). Never a command line argument.
//   STORM_AUTH_ACCOUNT  the AUTH account's public key (A...), or STORM_AUTH_ACCOUNT_FILE
//   STORM_ROLES         the role names every ticket asks for, comma separated (default app-feed-publish-orders,app-feed-subscribe-alarms)
//   STORM_TICKET_TTL_S  how long a ticket is valid for, in seconds (default 3600)
//   STORM_CONNECTIONS   how many connections per run (default 200)
//   STORM_RUNS          how many bursts (default 1)
//   STORM_PAUSE_MS      between runs, so each starts from a quiet broker (default 1500)
//   STORM_RAMP_MS       spread the starts evenly over this long; 0 starts them all at once (default 0)
//   STORM_TIMEOUT_MS    give up on a connection after this long (default 15000: the broker takes up to its authorization timeout, 10 s in
//                       these manifests, to refuse a ticket it cannot hand to the callout)
//   STORM_HOLD_MS       keep an admitted connection open this long before closing it (default 1000)
//   STORM_THREADS       worker threads (default 4)
//   STORM_LABEL         names the run in the report and as the `run` label of the metrics (default "run")
//   STORM_JSON          write the report to this file, or to stdout as one line starting STORM_REPORT_JSON when set to -; STORM_SAMPLES=1 adds every connection's timings
//   STORM_METRICS_PORT  serve Prometheus metrics on this port (default 0: off); STORM_METRICS_HOLD_S keeps serving this many seconds after the last run
//   STORM_CA_FILE       PEM file with the CA that signed the broker's certificate (required for wss://)

use std::collections::BTreeMap;
use std::fmt::Write as _;
mod ticket;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{Connector, client_async_tls_with_config};

use ticket::{Issuer, Ticket};

/// The line that starts the JSON report when STORM_JSON=- (stdout).
const REPORT_PREFIX: &str = "STORM_REPORT_JSON ";

type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name).ok().filter(|v| !v.is_empty()).unwrap_or_else(|| fallback.to_string())
}
fn env_num<T: std::str::FromStr>(name: &str, fallback: T) -> T {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(fallback)
}

/// A secret comes from NAME, or from the file named by NAME_FILE; empty if neither is set.
fn secret(name: &str) -> Res<String> {
    let v = env(name, "");
    if !v.is_empty() {
        return Ok(v);
    }
    let file = env(&format!("{name}_FILE"), "");
    if file.is_empty() {
        return Ok(String::new());
    }
    let text = std::fs::read_to_string(&file).map_err(|e| format!("cannot read {name}_FILE {file}: {e}"))?;
    Ok(text.trim_end_matches(['\r', '\n']).to_string())
}

struct Cfg {
    connections: usize,
    runs: usize,
    pause_ms: u64,
    ramp_ms: u64,
    timeout_ms: u64,
    hold_ms: u64,
    threads: usize,
    label: String,
    report: String,
    samples: bool,
    metrics_port: u16,
    metrics_hold_s: u64,
    ca_file: String,
    url: String,
    scoped_seed: String,
    auth_account: String,
    roles: Vec<String>,
    ticket_ttl_s: u64,
}

fn cfg() -> Res<Cfg> {
    let c = Cfg {
        connections: env_num("STORM_CONNECTIONS", 200),
        runs: env_num("STORM_RUNS", 1),
        pause_ms: env_num("STORM_PAUSE_MS", 1500),
        ramp_ms: env_num("STORM_RAMP_MS", 0),
        timeout_ms: env_num("STORM_TIMEOUT_MS", 15_000),
        hold_ms: env_num("STORM_HOLD_MS", 1000),
        threads: env_num("STORM_THREADS", 4),
        label: env("STORM_LABEL", "run"),
        report: env("STORM_JSON", ""),
        samples: env("STORM_SAMPLES", "") == "1",
        metrics_port: env_num("STORM_METRICS_PORT", 0),
        metrics_hold_s: env_num("STORM_METRICS_HOLD_S", 0),
        ca_file: env("STORM_CA_FILE", ""),
        url: env("STORM_URL", ""),
        scoped_seed: secret("STORM_SCOPED_SEED")?,
        auth_account: secret("STORM_AUTH_ACCOUNT")?,
        roles: env("STORM_ROLES", "app-feed-publish-orders,app-feed-subscribe-alarms").split(',').map(|r| r.trim().to_string()).filter(|r| !r.is_empty()).collect(),
        ticket_ttl_s: env_num("STORM_TICKET_TTL_S", 3600),
    };
    if c.url.is_empty() {
        return Err("set STORM_URL, the broker's WebSocket URL (ws:// or wss://)".into());
    }
    if c.scoped_seed.is_empty() || c.auth_account.is_empty() {
        return Err("set STORM_SCOPED_SEED (or STORM_SCOPED_SEED_FILE) and STORM_AUTH_ACCOUNT (or STORM_AUTH_ACCOUNT_FILE): the AUTH account's scoped signing seed and public key (components/provision makes them)".into());
    }
    Ok(c)
}

// ---- Prometheus metrics: counters, gauges and histograms rendered as text ----------------------------------------------------------------------------------------------------
const BUCKETS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]; // seconds

struct Hist {
    name: &'static str,
    help: &'static str,
    state: Mutex<([u64; 11], f64, u64)>,
}
impl Hist {
    fn new(name: &'static str, help: &'static str) -> Self {
        Hist { name, help, state: Mutex::new(([0; 11], 0.0, 0)) }
    }
    fn observe(&self, seconds: f64) {
        let mut g = self.state.lock().unwrap();
        g.1 += seconds;
        g.2 += 1;
        for (i, b) in BUCKETS.iter().enumerate() {
            if seconds <= *b {
                g.0[i] += 1;
            }
        }
    }
    fn render(&self, out: &mut String, run: &str) {
        let g = self.state.lock().unwrap();
        let _ = writeln!(out, "# HELP {} {}\n# TYPE {} histogram", self.name, self.help, self.name);
        for (i, b) in BUCKETS.iter().enumerate() {
            let _ = writeln!(out, "{}_bucket{{run=\"{run}\",le=\"{b}\"}} {}", self.name, g.0[i]);
        }
        let _ = writeln!(out, "{n}_bucket{{run=\"{run}\",le=\"+Inf\"}} {c}\n{n}_sum{{run=\"{run}\"}} {s}\n{n}_count{{run=\"{run}\"}} {c}", n = self.name, c = g.2, s = g.1);
    }
}

struct Metrics {
    run: String,
    admitted: AtomicU64,
    refused: AtomicU64,
    timeout: AtomicU64,
    error: AtomicU64,
    in_flight: AtomicI64,
    target: AtomicU64,
    sched_p99_us: AtomicU64,
    auth: Hist,
    total: Hist,
}
impl Metrics {
    fn new(run: String) -> Self {
        Metrics {
            run,
            admitted: AtomicU64::new(0),
            refused: AtomicU64::new(0),
            timeout: AtomicU64::new(0),
            error: AtomicU64::new(0),
            in_flight: AtomicI64::new(0),
            target: AtomicU64::new(0),
            sched_p99_us: AtomicU64::new(0),
            auth: Hist::new("storm_auth_seconds", "Time from sending CONNECT to the broker answering (PONG or -ERR)."),
            total: Hist::new("storm_total_seconds", "Time from starting a connection to the broker answering."),
        }
    }
    fn counter(&self, outcome: &str) -> &AtomicU64 {
        match outcome {
            "admitted" => &self.admitted,
            "refused" => &self.refused,
            "timeout" => &self.timeout,
            _ => &self.error,
        }
    }
    fn render(&self) -> String {
        let r = &self.run;
        let mut o = String::new();
        let _ = writeln!(o, "# HELP storm_connections_total Connections finished, by outcome.\n# TYPE storm_connections_total counter");
        for k in ["admitted", "refused", "timeout", "error"] {
            let _ = writeln!(o, "storm_connections_total{{run=\"{r}\",outcome=\"{k}\"}} {}", self.counter(k).load(Ordering::Relaxed));
        }
        let _ = writeln!(o, "# HELP storm_connections_in_flight Connections started and not yet answered.\n# TYPE storm_connections_in_flight gauge\nstorm_connections_in_flight{{run=\"{r}\"}} {}", self.in_flight.load(Ordering::Relaxed));
        let _ = writeln!(o, "# HELP storm_connections_target Connections each run will make.\n# TYPE storm_connections_target gauge\nstorm_connections_target{{run=\"{r}\"}} {}", self.target.load(Ordering::Relaxed));
        let _ = writeln!(o, "# HELP storm_scheduler_delay_p99_seconds How late the generator's runtime wakes a 10 ms timer (p99), in the last run.\n# TYPE storm_scheduler_delay_p99_seconds gauge\nstorm_scheduler_delay_p99_seconds{{run=\"{r}\"}} {}", self.sched_p99_us.load(Ordering::Relaxed) as f64 / 1e6);
        self.auth.render(&mut o, r);
        self.total.render(&mut o, r);
        o
    }
}

// A Prometheus endpoint is an HTTP GET that answers with this text.
async fn serve_metrics(port: u16, m: Arc<Metrics>) -> Res<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { continue };
            let m = m.clone();
            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};
                let mut buf = [0u8; 2048];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]);
                let resp = if head.starts_with("GET /metrics") {
                    let body = m.render();
                    format!("HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len())
                } else {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                };
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    Ok(())
}

// ---- one connection ---------------------------------------------------------------------------------------------------------------------------------------------------------------
struct Ctx {
    url: String,
    /// Makes each connection's ticket.
    issuer: Issuer,
    connector: Connector,
    timeout: Duration,
    metrics: Arc<Metrics>,
}

struct Rec {
    outcome: &'static str,
    detail: String,
    auth_ms: Option<f64>,
    total_ms: Option<f64>,
    open_ms: Option<f64>,
    /// When the connection started, when its CONNECT went out (None if it never did) and when its answer came, all in ms since the run began.
    /// With these a run's queue can be drawn: CONNECTs sent and not yet answered, over time.
    start_ms: f64,
    sent_ms: Option<f64>,
    done_ms: f64,
}

fn ms_since(t: Instant, began: Instant) -> f64 {
    t.saturating_duration_since(began).as_secs_f64() * 1000.0
}

/// One connection's timings for the report, to a tenth of a millisecond (a run of 1600 connections x 8 is then about 2 MB, not 3).
fn sample_json(r: &Rec) -> Value {
    let t = |v: Option<f64>| v.map(|x| (x * 10.0).round() / 10.0);
    let mut o = json!({"outcome": r.outcome, "startMs": t(Some(r.start_ms)), "sentMs": t(r.sent_ms), "doneMs": t(Some(r.done_ms)),
                       "authMs": t(r.auth_ms), "totalMs": t(r.total_ms), "openMs": t(r.open_ms)});
    if !r.detail.is_empty() {
        o["detail"] = json!(r.detail);
    }
    o
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

/// The CONNECT line for one connection: the ticket JWT and the broker's nonce signed with the ticket's user key.
fn connect_frame(jwt: &str, sig: &str) -> String {
    let c = json!({"verbose": false, "pedantic": false, "protocol": 1, "lang": "storm", "version": "0", "jwt": jwt, "sig": sig});
    format!("CONNECT {c}\r\nPING\r\n")
}

async fn attempt(ctx: &Ctx, ticket: &Ticket, start: Instant, sent_at: &Mutex<Option<Instant>>, open_ms: &Mutex<Option<f64>>) -> Res<(&'static str, String, Option<Ws>)> {
    let req = ctx.url.as_str().into_client_request()?;
    let uri = req.uri().clone();
    let host = uri.host().ok_or("no host in the broker URL")?.to_string();
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("wss") { 443 } else { 80 });
    let tcp = TcpStream::connect((host.as_str(), port)).await?;
    tcp.set_nodelay(true)?;
    let (mut ws, _) = client_async_tls_with_config(req, tcp, None, Some(ctx.connector.clone())).await?;
    *open_ms.lock().unwrap() = Some(start.elapsed().as_secs_f64() * 1000.0);

    let mut buf = String::new();
    let mut sent = false;
    while let Some(msg) = ws.next().await {
        let text = match msg? {
            Message::Text(t) => t.to_string(),
            Message::Binary(b) => String::from_utf8_lossy(&b).to_string(),
            Message::Close(_) => return Ok(("error", "closed before the broker answered".into(), None)),
            _ => continue,
        };
        buf.push_str(&text);
        if !sent {
            let Some(nl) = buf.find("\r\n") else { continue };
            if !buf.starts_with("INFO ") {
                continue;
            }
            // The nonce is read, and in operator mode signed, before CONNECT is built, so the signature is outside the "auth" interval that starts below.
            let Some(nonce) = serde_json::from_str::<Value>(&buf[5..nl]).ok().and_then(|v| v["nonce"].as_str().map(String::from)) else {
                return Ok(("error", "the broker sent no nonce: is it in operator mode?".into(), None));
            };
            buf.drain(..nl + 2);
            let frame = connect_frame(&ticket.jwt, &ticket.sign_nonce(&nonce));
            *sent_at.lock().unwrap() = Some(Instant::now());
            sent = true;
            ws.send(Message::text(frame)).await?;
            continue;
        }
        if let Some(i) = buf.find("-ERR") {
            let line = buf[i..].split("\r\n").next().unwrap_or("").to_string();
            return Ok(("refused", line, Some(ws)));
        }
        if buf.contains("PONG") {
            return Ok(("admitted", String::new(), Some(ws)));
        }
    }
    Ok(("error", "closed before the broker answered".into(), None))
}

async fn connect_once(ctx: Arc<Ctx>, ticket: Ticket, start: Instant, began: Instant) -> (Rec, Option<Ws>) {
    let m = &ctx.metrics;
    m.in_flight.fetch_add(1, Ordering::Relaxed);
    let sent_at = Mutex::new(None);
    let open_ms = Mutex::new(None);
    let result = tokio::time::timeout(ctx.timeout, attempt(&ctx, &ticket, start, &sent_at, &open_ms)).await;
    let now = Instant::now();
    let sent = *sent_at.lock().unwrap();
    let mut rec = Rec { outcome: "error", detail: String::new(), auth_ms: None, total_ms: Some(now.duration_since(start).as_secs_f64() * 1000.0), open_ms: *open_ms.lock().unwrap(),
                       start_ms: ms_since(start, began), sent_ms: sent.map(|s| ms_since(s, began)), done_ms: ms_since(now, began) };
    let mut ws = None;
    match result {
        Ok(Ok((outcome, detail, w))) => {
            rec.outcome = outcome;
            rec.detail = detail;
            ws = w;
        }
        Ok(Err(e)) => rec.detail = e.to_string(),
        Err(_) => {
            rec.outcome = "timeout";
            rec.detail = if sent.is_some() { "no PONG or -ERR" } else { "no INFO" }.into();
        }
    }
    if let Some(s) = sent {
        rec.auth_ms = Some(now.duration_since(s).as_secs_f64() * 1000.0);
    }
    m.in_flight.fetch_sub(1, Ordering::Relaxed);
    m.counter(rec.outcome).fetch_add(1, Ordering::Relaxed);
    if let (Some(a), true) = (rec.auth_ms, rec.outcome == "admitted" || rec.outcome == "refused") {
        m.auth.observe(a / 1000.0);
    }
    if let (Some(t), "admitted") = (rec.total_ms, rec.outcome) {
        m.total.observe(t / 1000.0);
    }
    (rec, ws)
}

// ---- the report -------------------------------------------------------------------------------------------------------------------------------------------------------------------
fn pct(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let i = ((p / 100.0 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len()) - 1;
    Some(sorted[i])
}
fn stats(values: &[f64]) -> Value {
    let mut s = values.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    json!({"n": s.len(), "p50": pct(&s, 50.0), "p90": pct(&s, 90.0), "p99": pct(&s, 99.0), "max": s.last()})
}
fn fmt(v: &Value) -> String {
    v.as_f64().map_or("     n/a".into(), |f| format!("{f:8.1}"))
}

async fn run_once(c: &Cfg, ctx: &Arc<Ctx>, index: usize) -> Value {
    let (tx, mut rx) = mpsc::unbounded_channel::<Rec>();
    ctx.metrics.target.store(c.connections as u64, Ordering::Relaxed);
    let hold = Duration::from_millis(c.hold_ms);

    // Every ticket is made now, before the clock starts and before the scheduling-delay ticker, so signing a JWT is neither in the
    // measured interval nor in the generator's own delay.
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let mut tickets = Vec::with_capacity(c.connections);
    for i in 0..c.connections {
        match ctx.issuer.ticket(&format!("storm-{i}"), now) {
            Ok(t) => tickets.push(t),
            Err(e) => {
                eprintln!("cannot make a ticket: {e}");
                std::process::exit(2);
            }
        }
    }
    let mut tickets = tickets.into_iter();

    // How late the runtime wakes a timer that asked for 10 ms: when this is large, the generator is the limit.
    let lags = Arc::new(Mutex::new(Vec::<u64>::new()));
    let ticker = {
        let lags = lags.clone();
        tokio::spawn(async move {
            loop {
                let t = Instant::now();
                tokio::time::sleep(Duration::from_millis(10)).await;
                let late = t.elapsed().saturating_sub(Duration::from_millis(10));
                lags.lock().unwrap().push(late.as_micros() as u64);
            }
        })
    };

    let began = Instant::now();
    let mut tasks = Vec::with_capacity(c.connections);
    for i in 0..c.connections {
        let (ctx, tx) = (ctx.clone(), tx.clone());
        let ticket = tickets.next().expect("one ticket per connection");
        let delay = if c.ramp_ms > 0 { Duration::from_millis(i as u64 * c.ramp_ms / c.connections as u64) } else { Duration::ZERO };
        let start_hint = Instant::now();
        tasks.push(tokio::spawn(async move {
            let start = if delay.is_zero() {
                start_hint
            } else {
                tokio::time::sleep(delay).await;
                Instant::now()
            };
            let (rec, ws) = connect_once(ctx, ticket, start, began).await;
            let _ = tx.send(rec);
            drop(tx); // the run's clock stops when the last answer is in, not when the last connection has been held and closed
            if let Some(mut ws) = ws {
                tokio::time::sleep(hold).await;
                let _ = ws.close(None).await;
            }
        }));
    }
    drop(tx);
    let mut records = Vec::with_capacity(c.connections);
    while let Some(r) = rx.recv().await {
        records.push(r);
    }
    let seconds = began.elapsed().as_secs_f64();
    ticker.abort();
    for t in tasks {
        let _ = t.await;
    }

    let mut lag = lags.lock().unwrap().clone();
    lag.sort_unstable();
    let lag_p99_ms = if lag.is_empty() { 0.0 } else { lag[((lag.len() as f64 * 0.99).ceil() as usize).clamp(1, lag.len()) - 1] as f64 / 1000.0 };
    ctx.metrics.sched_p99_us.store((lag_p99_ms * 1000.0) as u64, Ordering::Relaxed);

    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for r in &records {
        *counts.entry(r.outcome).or_default() += 1;
        if r.outcome != "admitted" {
            *reasons.entry(format!("{}: {}", r.outcome, r.detail)).or_default() += 1;
        }
    }
    let n = |k: &str| counts.get(k).copied().unwrap_or(0);
    let auth: Vec<f64> = records.iter().filter_map(|r| r.auth_ms).collect();
    let total: Vec<f64> = records.iter().filter(|r| r.outcome == "admitted").filter_map(|r| r.total_ms).collect();
    let (auth_s, total_s) = (stats(&auth), stats(&total));
    let success = if records.is_empty() { 0.0 } else { n("admitted") as f64 / records.len() as f64 };

    println!("\nrun {}/{}: admitted {}   refused {}   timeout {}   error {}   success {:.1}%   in {:.1} s ({:.0} connections/s)", index + 1, c.runs, n("admitted"), n("refused"), n("timeout"), n("error"), success * 100.0, seconds, c.connections as f64 / seconds);
    println!("                      n       p50       p90       p99       max   (ms)");
    println!("auth (CONNECT->PONG) {:4} {}  {}  {}  {}", auth_s["n"], fmt(&auth_s["p50"]), fmt(&auth_s["p90"]), fmt(&auth_s["p99"]), fmt(&auth_s["max"]));
    println!("total (start->PONG)  {:4} {}  {}  {}  {}", total_s["n"], fmt(&total_s["p50"]), fmt(&total_s["p90"]), fmt(&total_s["p99"]), fmt(&total_s["max"]));
    println!("generator scheduling delay p99: {lag_p99_ms:.1} ms (large means the generator, not the broker, is the limit)");
    for (reason, k) in &reasons {
        println!("  {k} x {reason}");
    }

    let mut report = json!({
        "run": c.label, "index": index + 1, "connections": c.connections, "rampMs": c.ramp_ms, "seconds": (seconds * 100.0).round() / 100.0,
        "outcomes": {"admitted": n("admitted"), "refused": n("refused"), "timeout": n("timeout"), "error": n("error")}, "successRate": success,
        "authMs": auth_s, "totalMs": total_s, "generatorSchedulingDelayP99Ms": lag_p99_ms, "failures": reasons,
    });
    if c.samples {
        report["samples"] = records.iter().map(sample_json).collect();
    }
    report
}

async fn real_main(c: Cfg) -> Res<i32> {
    let ca_pem = if c.ca_file.is_empty() { Vec::new() } else { std::fs::read(&c.ca_file).map_err(|e| format!("cannot read the CA file {}: {e}", c.ca_file))? };
    let _ = rustls::crypto::ring::default_provider().install_default();
    if c.url.starts_with("wss://") && ca_pem.is_empty() {
        return Err("wss:// needs STORM_CA_FILE, the PEM file of the CA that signed the broker's certificate".into());
    }

    let metrics = Arc::new(Metrics::new(c.label.clone()));
    if c.metrics_port != 0 {
        serve_metrics(c.metrics_port, metrics.clone()).await?;
        println!("metrics: http://0.0.0.0:{}/metrics (run=\"{}\")", c.metrics_port, c.label);
    }

    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_pem.as_slice()) {
        roots.add(cert?)?;
    }
    let tls = rustls::ClientConfig::builder().with_root_certificates(roots).with_no_client_auth();
    let issuer = Issuer::new(&c.scoped_seed, &c.auth_account, &c.roles, c.ticket_ttl_s)?;
    println!("tickets signed by scoped key {} for roles {:?}, valid {} s", issuer.public(), c.roles, c.ticket_ttl_s);
    let ctx = Arc::new(Ctx { url: c.url.clone(), issuer, connector: Connector::Rustls(Arc::new(tls)), timeout: Duration::from_millis(c.timeout_ms), metrics: metrics.clone() });

    println!("run \"{}\": {} connections x {} run(s) to {}, {}, {} worker thread(s)", c.label, c.connections, c.runs, c.url, if c.ramp_ms > 0 { format!("ramped over {} ms", c.ramp_ms) } else { "all at once".into() }, c.threads);

    let mut reports = Vec::new();
    let mut admitted = 0;
    for i in 0..c.runs {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(c.pause_ms)).await;
        }
        let r = run_once(&c, &ctx, i).await;
        admitted += r["outcomes"]["admitted"].as_u64().unwrap_or(0);
        reports.push(r);
    }

    if c.runs > 1 {
        let mut p50: Vec<f64> = reports.iter().filter_map(|r| r["authMs"]["p50"].as_f64()).collect();
        p50.sort_by(|a, b| a.partial_cmp(b).unwrap());
        if !p50.is_empty() {
            let mean = p50.iter().sum::<f64>() / p50.len() as f64;
            println!("\nacross {} runs, the median auth time: mean {:.1} ms, middle {:.1} ms, from {:.1} to {:.1} ms", p50.len(), mean, p50[p50.len() / 2], p50[0], p50[p50.len() - 1]);
        }
    }
    if !c.report.is_empty() {
        let out = if reports.len() == 1 { reports[0].clone() } else { json!({"run": c.label, "runs": reports}) };
        if c.report == "-" {
            // For a container: the report is one line on stdout, so `kubectl logs` collects it with the console report.
            println!("{REPORT_PREFIX}{out}");
        } else {
            std::fs::write(&c.report, serde_json::to_string_pretty(&out)? + "\n")?;
            println!("report written to {}", c.report);
        }
    }
    if c.metrics_port != 0 && c.metrics_hold_s > 0 {
        println!("serving the final metrics for {} s so Prometheus can scrape them", c.metrics_hold_s);
        tokio::time::sleep(Duration::from_secs(c.metrics_hold_s)).await;
    }
    Ok(if admitted == 0 { 1 } else { 0 })
}

fn main() {
    let c = match cfg() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(c.threads.max(1)).enable_all().build().expect("the runtime");
    match rt.block_on(real_main(c)) {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_carries_run_relative_times_to_a_tenth_of_a_millisecond() {
        let r = Rec { outcome: "admitted", detail: String::new(), auth_ms: Some(123.456), total_ms: Some(300.04), open_ms: Some(10.0),
                      start_ms: 0.04, sent_ms: Some(180.55), done_ms: 304.0 };
        let j = sample_json(&r);
        assert_eq!(j["startMs"], 0.0);
        assert_eq!(j["sentMs"], 180.6);
        assert_eq!(j["doneMs"], 304.0);
        assert_eq!(j["authMs"], 123.5);
        assert!(j.get("detail").is_none(), "an empty detail is left out");
        let never_sent = Rec { outcome: "error", detail: "closed before the broker answered".into(), auth_ms: None, total_ms: Some(5.0), open_ms: None,
                               start_ms: 1.0, sent_ms: None, done_ms: 6.0 };
        let j = sample_json(&never_sent);
        assert!(j["sentMs"].is_null() && j["authMs"].is_null());
        assert_eq!(j["detail"], "closed before the broker answered");
    }

    #[test]
    fn offsets_from_the_run_start_never_go_negative() {
        let began = Instant::now();
        let later = began + Duration::from_millis(250);
        assert!((ms_since(later, began) - 250.0).abs() < 1e-6);
        assert_eq!(ms_since(began, later), 0.0);
    }

    #[test]
    fn percentiles_use_the_nearest_rank() {
        let v: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(pct(&v, 50.0), Some(50.0));
        assert_eq!(pct(&v, 99.0), Some(99.0));
        assert_eq!(pct(&v, 100.0), Some(100.0));
        assert_eq!(pct(&[], 50.0), None);
        assert_eq!(pct(&[7.0], 99.0), Some(7.0));
    }

    #[test]
    fn stats_sort_their_input() {
        let s = stats(&[3.0, 1.0, 2.0]);
        assert_eq!(s["n"], 3);
        assert_eq!(s["p50"], 2.0);
        assert_eq!(s["max"], 3.0);
    }

    #[test]
    fn connect_frame_carries_the_ticket_and_the_signature_and_no_user_or_password() {
        let f = connect_frame("h.p.s", "sig");
        assert!(f.starts_with("CONNECT ") && f.ends_with("\r\nPING\r\n"));
        let body: Value = serde_json::from_str(f.trim_start_matches("CONNECT ").split("\r\n").next().unwrap()).unwrap();
        assert_eq!(body["jwt"], "h.p.s");
        assert_eq!(body["sig"], "sig");
        assert!(body.get("user").is_none() && body.get("pass").is_none());
        assert_eq!(body["protocol"], 1);
    }

    #[test]
    fn the_histogram_counts_cumulatively() {
        let h = Hist::new("h", "help");
        h.observe(0.004);
        h.observe(0.2);
        let mut out = String::new();
        h.render(&mut out, "r");
        assert!(out.contains("h_bucket{run=\"r\",le=\"0.005\"} 1"));
        assert!(out.contains("h_bucket{run=\"r\",le=\"0.25\"} 2"));
        assert!(out.contains("h_count{run=\"r\"} 2"));
    }

    #[test]
    fn metrics_render_never_includes_a_seed() {
        let m = Metrics::new("run".into());
        assert!(!m.render().contains("SA"));
    }
}
