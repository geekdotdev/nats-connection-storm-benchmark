// SPDX-License-Identifier: Apache-2.0
//
// A connection storm against a nats-server WebSocket listener: N connections at once (or spread over STORM_RAMP_MS), each timed from
// CONNECT to the broker's PONG or -ERR ("auth") and from the start of the connection to the PONG ("total").
//
// It is a separate process on purpose: it runs the connections on a pool of worker threads and reports its own scheduling delay (how late the
// runtime wakes a timer that asked for 10 ms), so a limit in the generator is visible instead of being counted as the broker's time.
//
// Authentication modes (STORM_AUTH):
//   plain  (default) connection i presents the user "u<i mod STORM_ACCOUNTS>" and the password STORM_PASSWORD. The broker's auth callout (components/callout)
//          answers by placing "u<N>" in account "A<N>".
//   none   no credentials, for a broker with no authentication; measures the generator itself.
//
//   cargo build --release     then run  target/release/auth-storm
//   STORM_URL=wss://broker:8443 STORM_CA_FILE=ca.pem STORM_PASSWORD_FILE=pw STORM_CONNECTIONS=800 STORM_RUNS=5 target/release/auth-storm
//
// Settings (environment):
//   STORM_URL           the broker's WebSocket URL, ws:// or wss:// (required)
//   STORM_AUTH          "plain" (default) or "none"
//   STORM_PASSWORD      the password every user presents; or STORM_PASSWORD_FILE, a file holding it (a mounted Secret). Never a command line argument.
//   STORM_ACCOUNTS      how many accounts the broker has: connection i is user u<i mod this> (default 16)
//   STORM_CONNECTIONS   how many connections per run (default 200)
//   STORM_RUNS          how many bursts (default 1)
//   STORM_PAUSE_MS      between runs, so each starts from a quiet broker (default 1500)
//   STORM_RAMP_MS       spread the starts evenly over this long; 0 starts them all at once (default 0)
//   STORM_TIMEOUT_MS    give up on a connection after this long (default 10000)
//   STORM_HOLD_MS       keep an admitted connection open this long before closing it (default 1000)
//   STORM_THREADS       worker threads (default 4)
//   STORM_LABEL         names the run in the report and as the `run` label of the metrics (default "run")
//   STORM_JSON          write the report to this file, or to stdout as one line starting STORM_REPORT_JSON when set to -; STORM_SAMPLES=1 adds every connection's timings
//   STORM_METRICS_PORT  serve Prometheus metrics on this port (default 0: off); STORM_METRICS_HOLD_S keeps serving this many seconds after the last run
//   STORM_CA_FILE       PEM file with the CA that signed the broker's certificate (required for wss://)

use std::collections::BTreeMap;
use std::fmt::Write as _;
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
    auth_none: bool,
    url: String,
    accounts: usize,
    password: String,
}

fn cfg() -> Res<Cfg> {
    let auth = env("STORM_AUTH", "plain");
    if auth != "plain" && auth != "none" {
        return Err("STORM_AUTH must be \"plain\" or \"none\"".into());
    }
    let c = Cfg {
        connections: env_num("STORM_CONNECTIONS", 200),
        runs: env_num("STORM_RUNS", 1),
        pause_ms: env_num("STORM_PAUSE_MS", 1500),
        ramp_ms: env_num("STORM_RAMP_MS", 0),
        timeout_ms: env_num("STORM_TIMEOUT_MS", 10_000),
        hold_ms: env_num("STORM_HOLD_MS", 1000),
        threads: env_num("STORM_THREADS", 4),
        label: env("STORM_LABEL", "run"),
        report: env("STORM_JSON", ""),
        samples: env("STORM_SAMPLES", "") == "1",
        metrics_port: env_num("STORM_METRICS_PORT", 0),
        metrics_hold_s: env_num("STORM_METRICS_HOLD_S", 0),
        ca_file: env("STORM_CA_FILE", ""),
        auth_none: auth == "none",
        url: env("STORM_URL", ""),
        accounts: env_num("STORM_ACCOUNTS", 16),
        password: secret("STORM_PASSWORD")?,
    };
    if c.url.is_empty() {
        return Err("set STORM_URL, the broker's WebSocket URL (ws:// or wss://)".into());
    }
    if !c.auth_none && c.password.is_empty() {
        return Err("set STORM_PASSWORD or STORM_PASSWORD_FILE (or STORM_AUTH=none)".into());
    }
    if c.accounts < 1 {
        return Err("STORM_ACCOUNTS must be at least 1".into());
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
    /// The password every user presents; None means no credentials.
    password: Option<String>,
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
}

type Ws = tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<TcpStream>>;

/// The CONNECT line for one connection: the user and password in plain mode, nothing extra with no credentials.
fn connect_frame(user: &str, password: Option<&str>) -> String {
    let mut c = json!({"verbose": false, "pedantic": false, "protocol": 1, "lang": "storm", "version": "0"});
    if let Some(p) = password {
        c["user"] = Value::String(user.to_string());
        c["pass"] = Value::String(p.to_string());
    }
    format!("CONNECT {c}\r\nPING\r\n")
}

async fn attempt(ctx: &Ctx, user: &str, start: Instant, sent_at: &Mutex<Option<Instant>>, open_ms: &Mutex<Option<f64>>) -> Res<(&'static str, String, Option<Ws>)> {
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
            buf.drain(..nl + 2);
            let frame = connect_frame(user, ctx.password.as_deref());
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

async fn connect_once(ctx: Arc<Ctx>, user: String, start: Instant) -> (Rec, Option<Ws>) {
    let m = &ctx.metrics;
    m.in_flight.fetch_add(1, Ordering::Relaxed);
    let sent_at = Mutex::new(None);
    let open_ms = Mutex::new(None);
    let result = tokio::time::timeout(ctx.timeout, attempt(&ctx, &user, start, &sent_at, &open_ms)).await;
    let now = Instant::now();
    let sent = *sent_at.lock().unwrap();
    let mut rec = Rec { outcome: "error", detail: String::new(), auth_ms: None, total_ms: Some(now.duration_since(start).as_secs_f64() * 1000.0), open_ms: *open_ms.lock().unwrap() };
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
        let user = format!("u{}", i % c.accounts);
        let delay = if c.ramp_ms > 0 { Duration::from_millis(i as u64 * c.ramp_ms / c.connections as u64) } else { Duration::ZERO };
        let start_hint = Instant::now();
        tasks.push(tokio::spawn(async move {
            let start = if delay.is_zero() {
                start_hint
            } else {
                tokio::time::sleep(delay).await;
                Instant::now()
            };
            let (rec, ws) = connect_once(ctx, user, start).await;
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
        report["samples"] = records.iter().map(|r| json!({"outcome": r.outcome, "detail": r.detail, "authMs": r.auth_ms, "totalMs": r.total_ms, "openMs": r.open_ms})).collect();
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
    let password = if c.auth_none { None } else { Some(c.password.clone()) };
    let ctx = Arc::new(Ctx { url: c.url.clone(), password, connector: Connector::Rustls(Arc::new(tls)), timeout: Duration::from_millis(c.timeout_ms), metrics: metrics.clone() });

    let who = if c.auth_none { "no credentials".to_string() } else { format!("users u0..u{} with a shared password", c.accounts - 1) };
    println!("run \"{}\": {} connections x {} run(s) to {} ({}), {}, {} worker thread(s)", c.label, c.connections, c.runs, c.url, who, if c.ramp_ms > 0 { format!("ramped over {} ms", c.ramp_ms) } else { "all at once".into() }, c.threads);

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
    fn connect_frame_carries_the_user_and_password_only_in_plain_mode() {
        let plain = connect_frame("u3", Some("pw"));
        assert!(plain.starts_with("CONNECT ") && plain.ends_with("\r\nPING\r\n"));
        let body: Value = serde_json::from_str(plain.trim_start_matches("CONNECT ").split("\r\n").next().unwrap()).unwrap();
        assert_eq!(body["user"], "u3");
        assert_eq!(body["pass"], "pw");
        let none = connect_frame("u3", None);
        assert!(!none.contains("\"user\"") && !none.contains("\"pass\""));
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
    fn metrics_render_never_includes_the_password() {
        let m = Metrics::new("run".into());
        assert!(!m.render().contains("pw"));
    }
}
