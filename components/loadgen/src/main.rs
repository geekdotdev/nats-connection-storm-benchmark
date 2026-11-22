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
//   STORM_CPUPROBE      host:port of the CPU probe (components/cpuprobe): at the start and the end of every burst the generator asks each probe for its
//                       containers' CPU counters, and the burst's report says how much CPU the broker, the callout and the generator used and whether
//                       they were throttled ("probeCpu"). Unset: not asked.
//   STORM_JSON          write the report to this file, or to stdout as one line starting STORM_REPORT_JSON when set to -; STORM_SAMPLES=1 adds every connection's timings
//   STORM_METRICS_PORT  serve Prometheus metrics on this port (default 0: off); STORM_METRICS_HOLD_S keeps serving this many seconds after the last run
//   STORM_CA_FILE       PEM file with the CA that signed the broker's certificate (required for wss://)
//
// Bad-faith connections (default 0.0, every existing run unaffected): each fraction is of STORM_CONNECTIONS, exact counts (rounded, not
// probabilistic), assigned in the fixed order below; whatever is left over is good faith, exactly as today. See plans/bad-faith-storm.md.
// The existing outcomes/authMs/totalMs/samples fields (and the Prometheus metrics) stay scoped to good-faith connections only, unchanged
// in meaning whether or not any bad faith is configured; a report with any configured carries a new "badFaith" object, one entry per
// kind actually present, with its own n/outcomes/authMs. None of the four is ever expected to be admitted: if one is, that is a finding.
//   STORM_INVALID_FRACTION               a garbage string where the JWT goes: fails to decode, the cheapest rejection
//   STORM_NONEXISTENT_ACCOUNT_FRACTION   a structurally perfect JWT whose issuer account was never provisioned: fails at account lookup
//   STORM_FORGED_FRACTION                a real, untouched, validly-issued ticket, but the nonce signed by an unrelated key: fails the
//                                        broker's LAST check, after every cheaper one already passed -- the costliest of the four
//   STORM_INCOMPLETE_HANDSHAKE_FRACTION  completes the WebSocket upgrade, receives INFO, then never sends CONNECT at all: the only kind
//                                        that gets no reply; silent until the broker's own authorization timeout closes it

use std::collections::BTreeMap;
use std::fmt::Write as _;
mod ticket;

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{Connector, client_async_tls_with_config};

use ticket::{Issuer, Ticket};

/// The line that starts the JSON report when STORM_JSON=- (stdout).
const REPORT_PREFIX: &str = "STORM_REPORT_JSON ";

// ---- bad-faith connections: invalid, forged, non-existent-account credentials, and a handshake with no CONNECT at all ---------------------
//
// Every kind here is confirmed against the broker's own source (nats-server 582fb8e939a7, server/auth.go and server/client.go), not assumed:
// every one of them ends at `c.authViolation()`, which sends `-ERR 'Authorization Violation'` then closes, EXCEPT incomplete-handshake,
// which is silent until the broker's own authorization timeout (10 s in these manifests) closes it with nothing sent at all. They are
// ordered here, as in plans/bad-faith-storm.md, from the cheapest rejection to the costliest:
//   invalid              a garbage string where the JWT goes: fails to decode, inside the broker's server lock, before anything else runs.
//   nonexistent-account  a structurally perfect, self-consistent JWT whose issuer account was never provisioned: fails at the broker's
//                        account lookup, after the lock is released.
//   forged               a genuinely real, untouched, validly-issued ticket -- the same JWT a good-faith connection presents -- but the
//                        CONNECT frame's nonce challenge is signed with an unrelated key instead of the one the ticket actually names.
//                        Every check ahead of the nonce-signature check (decode, account lookup, the scope's signature chain) succeeds;
//                        it fails at the LAST check in the whole chain, so it is the costliest of the four.
//   incomplete-handshake completes the WebSocket upgrade and receives the broker's INFO, then never sends CONNECT at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Invalid,
    NonexistentAccount,
    Forged,
    IncompleteHandshake,
    GoodFaith,
}
impl Kind {
    /// The order they are assigned in a burst (bad-faith first, by ascending cost, good-faith last) and the key a bad-faith kind's
    /// own stats are reported under (`badFaith`); `GoodFaith` has none, since it is what the existing top-level fields already mean.
    const BAD_FAITH_ORDER: [Kind; 4] = [Kind::Invalid, Kind::NonexistentAccount, Kind::Forged, Kind::IncompleteHandshake];
    fn report_key(&self) -> Option<&'static str> {
        match self {
            Kind::Invalid => Some("invalid"),
            Kind::NonexistentAccount => Some("nonexistentAccount"),
            Kind::Forged => Some("forged"),
            Kind::IncompleteHandshake => Some("incompleteHandshake"),
            Kind::GoodFaith => None,
        }
    }
}

/// How many of `total` connections are each bad-faith kind, from the configured fractions: rounded independently, then shrunk from the
/// costliest kind backwards if rounding would overshoot `total` (so the four kinds, plus whatever is left for good faith, always sum to
/// exactly `total`; none of this is probabilistic, so a configured fraction is also the exact count, not an average over many bursts).
fn bad_faith_counts(total: usize, fractions: [f64; 4]) -> [usize; 4] {
    let mut counts = fractions.map(|f| (total as f64 * f).round() as usize);
    let mut sum: usize = counts.iter().sum();
    let mut i = counts.len();
    while sum > total && i > 0 {
        i -= 1;
        if counts[i] > 0 {
            counts[i] -= 1;
            sum -= 1;
        }
    }
    counts
}

/// What one connection presents in CONNECT, decided before the burst starts (so deciding is not part of what is timed): a real ticket,
/// correctly or incorrectly signed (good-faith and nonexistent-account sign correctly, with their own ticket's key; forged does not), a
/// garbage string that is not a JWT at all (invalid), or nothing -- never send CONNECT (incomplete-handshake).
enum Credential {
    Ticket { ticket: Ticket, correct_signature: bool },
    Garbage,
    NoConnect,
}

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
    cpuprobe: String,
    metrics_port: u16,
    metrics_hold_s: u64,
    ca_file: String,
    url: String,
    scoped_seed: String,
    auth_account: String,
    roles: Vec<String>,
    ticket_ttl_s: u64,
    bad_faith: [f64; 4], // order: Kind::BAD_FAITH_ORDER (invalid, nonexistent-account, forged, incomplete-handshake)
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
        cpuprobe: env("STORM_CPUPROBE", ""),
        metrics_port: env_num("STORM_METRICS_PORT", 0),
        metrics_hold_s: env_num("STORM_METRICS_HOLD_S", 0),
        ca_file: env("STORM_CA_FILE", ""),
        url: env("STORM_URL", ""),
        scoped_seed: secret("STORM_SCOPED_SEED")?,
        auth_account: secret("STORM_AUTH_ACCOUNT")?,
        roles: env("STORM_ROLES", "app-feed-publish-orders,app-feed-subscribe-alarms").split(',').map(|r| r.trim().to_string()).filter(|r| !r.is_empty()).collect(),
        ticket_ttl_s: env_num("STORM_TICKET_TTL_S", 3600),
        bad_faith: [
            env_num("STORM_INVALID_FRACTION", 0.0),
            env_num("STORM_NONEXISTENT_ACCOUNT_FRACTION", 0.0),
            env_num("STORM_FORGED_FRACTION", 0.0),
            env_num("STORM_INCOMPLETE_HANDSHAKE_FRACTION", 0.0),
        ],
    };
    if c.url.is_empty() {
        return Err("set STORM_URL, the broker's WebSocket URL (ws:// or wss://)".into());
    }
    if c.scoped_seed.is_empty() || c.auth_account.is_empty() {
        return Err("set STORM_SCOPED_SEED (or STORM_SCOPED_SEED_FILE) and STORM_AUTH_ACCOUNT (or STORM_AUTH_ACCOUNT_FILE): the AUTH account's scoped signing seed and public key (components/provision makes them)".into());
    }
    for f in c.bad_faith {
        if !(0.0..=1.0).contains(&f) {
            return Err("STORM_INVALID_FRACTION, STORM_NONEXISTENT_ACCOUNT_FRACTION, STORM_FORGED_FRACTION and STORM_INCOMPLETE_HANDSHAKE_FRACTION must each be 0.0 to 1.0".into());
        }
    }
    if c.bad_faith.iter().sum::<f64>() > 1.0 {
        return Err("STORM_INVALID_FRACTION + STORM_NONEXISTENT_ACCOUNT_FRACTION + STORM_FORGED_FRACTION + STORM_INCOMPLETE_HANDSHAKE_FRACTION must not exceed 1.0: whatever is left over is good faith".into());
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
    /// Makes each good-faith (and forged, which presents a real ticket too) connection's ticket.
    issuer: Issuer,
    /// Makes a nonexistent-account connection's ticket: self-consistent, but never the real AUTH account.
    rogue_issuer: Issuer,
    connector: Connector,
    timeout: Duration,
    metrics: Arc<Metrics>,
}

struct Rec {
    kind: Kind,
    outcome: &'static str,
    detail: String,
    auth_ms: Option<f64>,
    total_ms: Option<f64>,
    open_ms: Option<f64>,
    /// When the connection started, when its CONNECT went out (None if it never did) and when its answer came, all in ms since the run began.
    /// With these a run's queue can be drawn: CONNECTs sent and not yet answered, over time. For `incomplete-handshake`, which never sends
    /// CONNECT, `sent_ms` is set anyway, to when INFO arrived: so `auth_ms` (answer time minus sent time) means, for that kind only, how
    /// long the broker held the connection open after the point CONNECT would have gone out -- the quantity that kind exists to measure.
    start_ms: f64,
    sent_ms: Option<f64>,
    done_ms: f64,
}

/// This container's CPU use and throttling, from its cgroup (v2): the counters in `cpu.stat` and the limit in `cpu.max`. Read at the start
/// and the end of a burst, their difference says how much CPU the generator used during it and whether the limit held it back: a generator
/// that is throttled, or close to its limit, delays the very connections it is timing.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct CpuStat {
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
    nr_periods: u64,
    nr_throttled: u64,
    throttled_usec: u64,
}

fn parse_cpu_stat(text: &str) -> Option<CpuStat> {
    let mut s = CpuStat::default();
    let mut seen = false;
    for line in text.lines() {
        let mut it = line.split_whitespace();
        let (Some(k), Some(v)) = (it.next(), it.next().and_then(|v| v.parse::<u64>().ok())) else { continue };
        match k {
            "usage_usec" => {
                s.usage_usec = v;
                seen = true;
            }
            "user_usec" => s.user_usec = v,
            "system_usec" => s.system_usec = v,
            "nr_periods" => s.nr_periods = v,
            "nr_throttled" => s.nr_throttled = v,
            "throttled_usec" => s.throttled_usec = v,
            _ => {}
        }
    }
    seen.then_some(s)
}

/// The CPU limit in cores from `cpu.max` ("max 100000" is no limit, "200000 100000" is two cores).
fn parse_cpu_max(text: &str) -> Option<f64> {
    let mut it = text.split_whitespace();
    let (quota, period) = (it.next()?, it.next()?);
    let (quota, period): (f64, f64) = (quota.parse().ok()?, period.parse().ok()?);
    (period > 0.0).then_some(quota / period)
}

fn read_cgroup_cpu() -> Option<(CpuStat, Option<f64>)> {
    let stat = parse_cpu_stat(&std::fs::read_to_string("/sys/fs/cgroup/cpu.stat").ok()?)?;
    let limit = std::fs::read_to_string("/sys/fs/cgroup/cpu.max").ok().and_then(|t| parse_cpu_max(&t));
    Some((stat, limit))
}

/// The CPU used between two readings, for the report: JSON null when either reading failed (cgroup v1, or not a container).
fn cpu_delta_json(before: Option<(CpuStat, Option<f64>)>, after: Option<(CpuStat, Option<f64>)>) -> Value {
    let (Some((b, _)), Some((a, limit))) = (before, after) else { return Value::Null };
    let d = |x: u64, y: u64| y.saturating_sub(x);
    json!({"usageUsec": d(b.usage_usec, a.usage_usec), "userUsec": d(b.user_usec, a.user_usec), "systemUsec": d(b.system_usec, a.system_usec),
           "periods": d(b.nr_periods, a.nr_periods), "throttledPeriods": d(b.nr_throttled, a.nr_throttled),
           "throttledUsec": d(b.throttled_usec, a.throttled_usec), "limitCores": limit})
}

// ---- the CPU probe: the broker's and the callout's CPU in a burst -------------------------------------------------------------------------------------------

/// One container's cumulative counters as a probe reported them.
#[derive(Clone, Debug, PartialEq)]
struct ProbeReading {
    component: String,
    pod: String,
    container: String,
    usage_usec: u64,
    user_usec: u64,
    system_usec: u64,
    periods: u64,
    throttled_periods: u64,
    throttled_usec: u64,
    limit_cores: Option<f64>,
}

/// What every probe answered at one moment. `probes` is how many were asked (one per node); `errors` says why some did not answer.
#[derive(Clone, Debug, Default)]
struct ProbeSnapshot {
    readings: Vec<ProbeReading>,
    probes: usize,
    errors: Vec<String>,
}

/// The body of an HTTP response, or why it is not one: a non-200 status, or no header end.
fn parse_http_body(buf: &[u8]) -> Result<String, String> {
    let text = String::from_utf8_lossy(buf);
    let (head, body) = text.split_once("\r\n\r\n").ok_or("no end of headers")?;
    let status = head.lines().next().unwrap_or("");
    if status.split_whitespace().nth(1) != Some("200") {
        return Err(format!("status: {status}"));
    }
    Ok(body.to_string())
}

/// What `GET /counters` of a probe returns: {"node", "timeMs", "containers": [{namespace, pod, container, component, usageUsec, ...}]}.
fn parse_probe_doc(body: &str) -> Result<Vec<ProbeReading>, String> {
    let doc: Value = serde_json::from_str(body).map_err(|e| e.to_string())?;
    let containers = doc["containers"].as_array().ok_or("no containers array")?;
    let n = |c: &Value, k: &str| c[k].as_u64().unwrap_or(0);
    let text = |c: &Value, k: &str| c[k].as_str().unwrap_or("").to_string();
    Ok(containers
        .iter()
        .map(|c| ProbeReading {
            component: text(c, "component"),
            pod: text(c, "pod"),
            container: text(c, "container"),
            usage_usec: n(c, "usageUsec"),
            user_usec: n(c, "userUsec"),
            system_usec: n(c, "systemUsec"),
            periods: n(c, "periods"),
            throttled_periods: n(c, "throttledPeriods"),
            throttled_usec: n(c, "throttledUsec"),
            limit_cores: c["limitCores"].as_f64(),
        })
        .collect())
}

/// GET a path over HTTP/1.0 (so the server does not chunk the answer) and return the body, within `timeout`.
async fn http_get(addr: SocketAddr, path: &str, timeout: Duration) -> Result<String, String> {
    let fetch = async {
        let mut s = TcpStream::connect(addr).await?;
        s.write_all(format!("GET {path} HTTP/1.0\r\n\r\n").as_bytes()).await?;
        let mut buf = Vec::new();
        s.read_to_end(&mut buf).await?;
        Ok::<_, std::io::Error>(buf)
    };
    let buf = tokio::time::timeout(timeout, fetch).await.map_err(|_| "timed out".to_string())?.map_err(|e| e.to_string())?;
    parse_http_body(&buf)
}

/// Ask every probe behind `host_port` (a headless Service name resolves to all of them) for its counters, all at once.
async fn read_probes(host_port: &str) -> Option<ProbeSnapshot> {
    if host_port.is_empty() {
        return None;
    }
    let addrs: Vec<SocketAddr> = match tokio::net::lookup_host(host_port).await {
        Ok(a) => a.collect(),
        Err(e) => return Some(ProbeSnapshot { errors: vec![format!("resolving {host_port}: {e}")], ..Default::default() }),
    };
    let tasks: Vec<_> = addrs.iter().map(|a| {
        let a = *a;
        tokio::spawn(async move { (a, http_get(a, "/counters", Duration::from_millis(500)).await) })
    }).collect();
    let mut snap = ProbeSnapshot { probes: addrs.len(), ..Default::default() };
    for t in tasks {
        match t.await {
            Ok((a, Ok(body))) => match parse_probe_doc(&body) {
                Ok(r) => snap.readings.extend(r),
                Err(e) => snap.errors.push(format!("{a}: {e}")),
            },
            Ok((a, Err(e))) => snap.errors.push(format!("{a}: {e}")),
            Err(e) => snap.errors.push(e.to_string()),
        }
    }
    Some(snap)
}

/// The CPU the containers used between two snapshots, for the report. A container missing from either snapshot, or whose counters went
/// backwards (it restarted), is left out. Null when the probe was not asked or nothing answered either time.
fn probe_delta_json(before: &Option<ProbeSnapshot>, after: &Option<ProbeSnapshot>) -> Value {
    let (Some(b), Some(a)) = (before, after) else { return Value::Null };
    if b.readings.is_empty() || a.readings.is_empty() {
        return json!({"probes": a.probes, "errors": b.errors.iter().chain(a.errors.iter()).collect::<Vec<_>>(), "containers": []});
    }
    let mut containers = Vec::new();
    for x in &a.readings {
        let Some(y) = b.readings.iter().find(|y| y.pod == x.pod && y.container == x.container) else { continue };
        if x.usage_usec < y.usage_usec || x.periods < y.periods {
            continue;
        }
        containers.push(json!({
            "component": x.component, "pod": x.pod, "container": x.container,
            "usageUsec": x.usage_usec - y.usage_usec, "userUsec": x.user_usec.saturating_sub(y.user_usec),
            "systemUsec": x.system_usec.saturating_sub(y.system_usec), "periods": x.periods - y.periods,
            "throttledPeriods": x.throttled_periods.saturating_sub(y.throttled_periods),
            "throttledUsec": x.throttled_usec.saturating_sub(y.throttled_usec), "limitCores": x.limit_cores,
        }));
    }
    json!({"probes": a.probes, "errors": b.errors.iter().chain(a.errors.iter()).collect::<Vec<_>>(), "containers": containers})
}

fn epoch_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
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

async fn attempt(ctx: &Ctx, cred: &Credential, start: Instant, sent_at: &Mutex<Option<Instant>>, open_ms: &Mutex<Option<f64>>) -> Res<(&'static str, String, Option<Ws>)> {
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
            match cred {
                Credential::NoConnect => {
                    // Deliberately never send CONNECT (incomplete-handshake): mark the point it would have gone out, so `auth_ms` below
                    // ends up measuring how long the broker holds this connection open afterward, and keep reading (nothing further is
                    // expected but the broker's own eventual close), without setting `sent`.
                    *sent_at.lock().unwrap() = Some(Instant::now());
                    continue;
                }
                Credential::Garbage => {
                    // The signature is never checked here: the broker fails to decode the JWT first, before it ever looks at `sig`.
                    let frame = connect_frame("not-a-jwt-at-all", &ticket::sign_nonce_with_unrelated_key(&nonce)?);
                    *sent_at.lock().unwrap() = Some(Instant::now());
                    sent = true;
                    ws.send(Message::text(frame)).await?;
                }
                Credential::Ticket { ticket, correct_signature } => {
                    let sig = if *correct_signature { ticket.sign_nonce(&nonce) } else { ticket::sign_nonce_with_unrelated_key(&nonce)? };
                    let frame = connect_frame(&ticket.jwt, &sig);
                    *sent_at.lock().unwrap() = Some(Instant::now());
                    sent = true;
                    ws.send(Message::text(frame)).await?;
                }
            }
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

async fn connect_once(ctx: Arc<Ctx>, kind: Kind, cred: Credential, start: Instant, began: Instant) -> (Rec, Option<Ws>) {
    let m = &ctx.metrics;
    let good_faith = kind == Kind::GoodFaith;
    if good_faith {
        m.in_flight.fetch_add(1, Ordering::Relaxed);
    }
    let sent_at = Mutex::new(None);
    let open_ms = Mutex::new(None);
    let result = tokio::time::timeout(ctx.timeout, attempt(&ctx, &cred, start, &sent_at, &open_ms)).await;
    let now = Instant::now();
    let sent = *sent_at.lock().unwrap();
    let mut rec = Rec { kind, outcome: "error", detail: String::new(), auth_ms: None, total_ms: Some(now.duration_since(start).as_secs_f64() * 1000.0), open_ms: *open_ms.lock().unwrap(),
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
            rec.detail = if kind == Kind::IncompleteHandshake {
                "no response within the generator's own timeout (the broker's authorization timeout did not close it first)"
            } else if sent.is_some() {
                "no PONG or -ERR"
            } else {
                "no INFO"
            }.into();
        }
    }
    if let Some(s) = sent {
        rec.auth_ms = Some(now.duration_since(s).as_secs_f64() * 1000.0);
    }
    if good_faith {
        m.in_flight.fetch_sub(1, Ordering::Relaxed);
        m.counter(rec.outcome).fetch_add(1, Ordering::Relaxed);
        if let (Some(a), true) = (rec.auth_ms, rec.outcome == "admitted" || rec.outcome == "refused") {
            m.auth.observe(a / 1000.0);
        }
        if let (Some(t), "admitted") = (rec.total_ms, rec.outcome) {
            m.total.observe(t / 1000.0);
        }
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

/// Outcome counts, failure reasons, and auth/total percentiles and the success rate, over whichever records are passed in: the
/// good-faith subset for the report's own top-level fields, or one bad-faith kind's subset for its entry in `badFaith`.
fn outcome_stats(records: &[&Rec]) -> (BTreeMap<&'static str, usize>, BTreeMap<String, usize>, Value, Value, f64) {
    let mut counts: BTreeMap<&str, usize> = BTreeMap::new();
    let mut reasons: BTreeMap<String, usize> = BTreeMap::new();
    for r in records {
        *counts.entry(r.outcome).or_default() += 1;
        if r.outcome != "admitted" {
            *reasons.entry(format!("{}: {}", r.outcome, r.detail)).or_default() += 1;
        }
    }
    let n = |k: &str| counts.get(k).copied().unwrap_or(0);
    let auth: Vec<f64> = records.iter().filter_map(|r| r.auth_ms).collect();
    let total: Vec<f64> = records.iter().filter(|r| r.outcome == "admitted").filter_map(|r| r.total_ms).collect();
    let success = if records.is_empty() { 0.0 } else { n("admitted") as f64 / records.len() as f64 };
    (counts, reasons, stats(&auth), stats(&total), success)
}

/// The kind of each of `total` connections, in the fixed order (bad faith first, ascending cost; good faith fills the rest) and the
/// real or rogue ticket each needs -- or none, for `invalid` and `incomplete-handshake`. Built before the clock starts, like the real
/// tickets always were: making a ticket, or deciding there is none to make, is never part of what a burst measures.
fn kinds_and_credentials(c: &Cfg, ctx: &Ctx, now: u64) -> Res<Vec<(Kind, Credential)>> {
    let counts = bad_faith_counts(c.connections, c.bad_faith);
    let mut out = Vec::with_capacity(c.connections);
    for (kind, count) in Kind::BAD_FAITH_ORDER.into_iter().zip(counts) {
        for i in 0..count {
            let cred = match kind {
                Kind::Invalid => Credential::Garbage,
                Kind::IncompleteHandshake => Credential::NoConnect,
                Kind::NonexistentAccount => Credential::Ticket { ticket: ctx.rogue_issuer.ticket(&format!("storm-bad-{i}"), now)?, correct_signature: true },
                Kind::Forged => Credential::Ticket { ticket: ctx.issuer.ticket(&format!("storm-bad-{i}"), now)?, correct_signature: false },
                Kind::GoodFaith => unreachable!("not in BAD_FAITH_ORDER"),
            };
            out.push((kind, cred));
        }
    }
    for i in out.len()..c.connections {
        out.push((Kind::GoodFaith, Credential::Ticket { ticket: ctx.issuer.ticket(&format!("storm-{i}"), now)?, correct_signature: true }));
    }
    Ok(out)
}

/// Whether the burst's own clock and CPU reading should be taken now: once the good-faith subset has all answered (`good_faith_total >
/// 0`, regardless of how many bad-faith stragglers are still outstanding -- `records_so_far` can be less than `total`), or, when no
/// good faith was configured at all, once everything has (a bad-faith-only calibration burst, where there is no earlier point to use).
fn ready_to_snapshot(good_faith_seen: usize, good_faith_total: usize, records_so_far: usize, total: usize) -> bool {
    if good_faith_total > 0 { good_faith_seen == good_faith_total } else { records_so_far == total }
}

async fn run_once(c: &Cfg, ctx: &Arc<Ctx>, index: usize) -> Value {
    let (tx, mut rx) = mpsc::unbounded_channel::<Rec>();
    ctx.metrics.target.store(c.connections as u64, Ordering::Relaxed);
    let hold = Duration::from_millis(c.hold_ms);

    // Every connection's credential is made now, before the clock starts and before the scheduling-delay ticker, so making it (a real
    // ticket's signing, or deciding a bad-faith connection needs none) is neither in the measured interval nor in the generator's own delay.
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    let mut creds = match kinds_and_credentials(c, ctx, now) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("cannot make a ticket: {e}");
            std::process::exit(2);
        }
    };
    let good_faith_total = creds.iter().filter(|(k, _)| *k == Kind::GoodFaith).count();
    let mut creds = creds.drain(..);

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

    let probe_before = read_probes(&c.cpuprobe).await;   // before the burst's clock starts: asking takes a few milliseconds that are not part of it
    let cpu_before = read_cgroup_cpu();
    let started_at_ms = epoch_ms();
    let began = Instant::now();
    let mut tasks = Vec::with_capacity(c.connections);
    for i in 0..c.connections {
        let (ctx, tx) = (ctx.clone(), tx.clone());
        let (kind, cred) = creds.next().expect("one credential per connection");
        let delay = if c.ramp_ms > 0 { Duration::from_millis(i as u64 * c.ramp_ms / c.connections as u64) } else { Duration::ZERO };
        let start_hint = Instant::now();
        tasks.push(tokio::spawn(async move {
            let start = if delay.is_zero() {
                start_hint
            } else {
                tokio::time::sleep(delay).await;
                Instant::now()
            };
            let (rec, ws) = connect_once(ctx, kind, cred, start, began).await;
            let _ = tx.send(rec);
            drop(tx); // the run's clock stops when the last answer is in, not when the last connection has been held and closed
            if let Some(mut ws) = ws {
                tokio::time::sleep(hold).await;
                let _ = ws.close(None).await;
            }
        }));
    }
    drop(tx);
    // The burst's own clock, and the CPU it is measured against, stop when the good-faith subset has all answered -- not when every
    // bad-faith straggler has (an incomplete-handshake connection can sit for the broker's whole 10 s authorization timeout, which
    // would otherwise balloon "seconds" regardless of how fast real traffic was actually served, breaking comparability with every
    // run before bad faith existed). With no good faith configured at all (a bad-faith-only calibration burst), the old meaning --
    // after everything -- is what is left to use.
    let mut records = Vec::with_capacity(c.connections);
    let mut good_faith_seen = 0usize;
    let mut snapshot: Option<(f64, Value, u64, Value)> = None;
    while let Some(r) = rx.recv().await {
        if r.kind == Kind::GoodFaith {
            good_faith_seen += 1;
        }
        records.push(r);
        if snapshot.is_none() && ready_to_snapshot(good_faith_seen, good_faith_total, records.len(), c.connections) {
            let seconds = began.elapsed().as_secs_f64();
            let cpu = cpu_delta_json(cpu_before, read_cgroup_cpu());
            let ended_at_ms = epoch_ms();
            let probe_cpu = probe_delta_json(&probe_before, &read_probes(&c.cpuprobe).await);
            snapshot = Some((seconds, cpu, ended_at_ms, probe_cpu));
        }
    }
    let (seconds, cpu, ended_at_ms, probe_cpu) = snapshot.unwrap_or_else(|| (began.elapsed().as_secs_f64(), Value::Null, epoch_ms(), Value::Null));
    ticker.abort();
    for t in tasks {
        let _ = t.await;
    }

    let mut lag = lags.lock().unwrap().clone();
    lag.sort_unstable();
    let lag_p99_ms = if lag.is_empty() { 0.0 } else { lag[((lag.len() as f64 * 0.99).ceil() as usize).clamp(1, lag.len()) - 1] as f64 / 1000.0 };
    ctx.metrics.sched_p99_us.store((lag_p99_ms * 1000.0) as u64, Ordering::Relaxed);

    // Every existing field below (outcomes, authMs, totalMs, successRate, failures, samples) is scoped to good faith only, whether or
    // not any bad faith was configured: a plain run's report is unchanged, byte for byte, down to the absence of the new fields below.
    let good: Vec<&Rec> = records.iter().filter(|r| r.kind == Kind::GoodFaith).collect();
    let (counts, reasons, auth_s, total_s, success) = outcome_stats(&good);
    let n = |k: &str| counts.get(k).copied().unwrap_or(0);

    println!("\nrun {}/{}: admitted {}   refused {}   timeout {}   error {}   success {:.1}%   in {:.1} s ({:.0} connections/s)", index + 1, c.runs, n("admitted"), n("refused"), n("timeout"), n("error"), success * 100.0, seconds, good.len() as f64 / seconds);
    println!("                      n       p50       p90       p99       max   (ms)");
    println!("auth (CONNECT->PONG) {:4} {}  {}  {}  {}", auth_s["n"], fmt(&auth_s["p50"]), fmt(&auth_s["p90"]), fmt(&auth_s["p99"]), fmt(&auth_s["max"]));
    println!("total (start->PONG)  {:4} {}  {}  {}  {}", total_s["n"], fmt(&total_s["p50"]), fmt(&total_s["p90"]), fmt(&total_s["p99"]), fmt(&total_s["max"]));
    println!("generator scheduling delay p99: {lag_p99_ms:.1} ms (large means the generator, not the broker, is the limit)");
    for (reason, k) in &reasons {
        println!("  {k} x {reason}");
    }

    let bad_faith_present: Vec<Kind> = Kind::BAD_FAITH_ORDER.into_iter().filter(|k| records.iter().any(|r| r.kind == *k)).collect();
    let mut bad_faith_report = serde_json::Map::new();
    if !bad_faith_present.is_empty() {
        println!("bad faith (none of these are expected to be admitted):");
        println!("  {:<19}  n       p50       p90       p99       max   (ms)   admitted refused timeout error", "");
        for kind in &bad_faith_present {
            let subset: Vec<&Rec> = records.iter().filter(|r| r.kind == *kind).collect();
            let (k_counts, k_reasons, k_auth, k_total, _) = outcome_stats(&subset);
            let kn = |o: &str| k_counts.get(o).copied().unwrap_or(0);
            println!("  {:<19} {:4} {}  {}  {}  {}      {:8} {:7} {:7} {:5}", kind.report_key().unwrap(), k_auth["n"], fmt(&k_auth["p50"]), fmt(&k_auth["p90"]), fmt(&k_auth["p99"]), fmt(&k_auth["max"]), kn("admitted"), kn("refused"), kn("timeout"), kn("error"));
            if kn("admitted") > 0 {
                println!("  ^ {} of {} {} connections were ADMITTED: that should never happen", kn("admitted"), subset.len(), kind.report_key().unwrap());
            }
            bad_faith_report.insert(kind.report_key().unwrap().to_string(), json!({
                "n": subset.len(), "outcomes": {"admitted": kn("admitted"), "refused": kn("refused"), "timeout": kn("timeout"), "error": kn("error")},
                "authMs": k_auth, "totalMs": k_total, "reasons": k_reasons,
            }));
        }
    }

    let mut report = json!({
        "run": c.label, "index": index + 1, "connections": c.connections, "rampMs": c.ramp_ms, "seconds": (seconds * 100.0).round() / 100.0,
        "outcomes": {"admitted": n("admitted"), "refused": n("refused"), "timeout": n("timeout"), "error": n("error")}, "successRate": success,
        "authMs": auth_s, "totalMs": total_s, "generatorSchedulingDelayP99Ms": lag_p99_ms, "failures": reasons,
    });
    report["cgroupCpu"] = cpu;
    report["probeCpu"] = probe_cpu;
    report["startedAtMs"] = json!(started_at_ms);   // wall clock, so the burst can be found in Prometheus's series
    report["endedAtMs"] = json!(ended_at_ms);
    if !bad_faith_present.is_empty() {
        report["goodFaithConnections"] = json!(good.len());
        report["badFaithConnections"] = json!(c.connections - good.len());
        report["badFaith"] = Value::Object(bad_faith_report);
    }
    if c.samples {
        report["samples"] = good.iter().map(|r| sample_json(r)).collect();
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
    // Built once and reused for every nonexistent-account connection in every burst: one account the broker will never find, not a
    // fresh one per connection (that would work too, but there is nothing to gain from it and it would cost a key generation each time).
    let rogue_issuer = Issuer::random(&c.roles, c.ticket_ttl_s)?;
    if c.bad_faith.iter().any(|f| *f > 0.0) {
        println!("bad faith configured: invalid {:.2}, nonexistent-account {:.2} (account {}), forged {:.2}, incomplete-handshake {:.2}",
                c.bad_faith[0], c.bad_faith[1], rogue_issuer.public(), c.bad_faith[2], c.bad_faith[3]);
    }
    let ctx = Arc::new(Ctx { url: c.url.clone(), issuer, rogue_issuer, connector: Connector::Rustls(Arc::new(tls)), timeout: Duration::from_millis(c.timeout_ms), metrics: metrics.clone() });

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
        let r = Rec { kind: Kind::GoodFaith, outcome: "admitted", detail: String::new(), auth_ms: Some(123.456), total_ms: Some(300.04), open_ms: Some(10.0),
                      start_ms: 0.04, sent_ms: Some(180.55), done_ms: 304.0 };
        let j = sample_json(&r);
        assert_eq!(j["startMs"], 0.0);
        assert_eq!(j["sentMs"], 180.6);
        assert_eq!(j["doneMs"], 304.0);
        assert_eq!(j["authMs"], 123.5);
        assert!(j.get("detail").is_none(), "an empty detail is left out");
        let never_sent = Rec { kind: Kind::GoodFaith, outcome: "error", detail: "closed before the broker answered".into(), auth_ms: None, total_ms: Some(5.0), open_ms: None,
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

    const CPU_STAT_V2: &str = "usage_usec 1812754\nuser_usec 1118932\nsystem_usec 693821\ncore_sched.force_idle_usec 0\nnr_periods 40\nnr_throttled 7\nthrottled_usec 350000\nnr_bursts 0\nburst_usec 0\n";

    const PROBE_BODY: &str = r#"{"node":"n1","timeMs":1,"containers":[
        {"namespace":"storm-bench","pod":"broker-0","container":"broker","component":"broker","usageUsec":5000000,"userUsec":3000000,"systemUsec":2000000,"periods":100,"throttledPeriods":3,"throttledUsec":40000,"limitCores":3},
        {"namespace":"storm-bench","pod":"callout-x","container":"callout","component":"callout","usageUsec":1000000,"userUsec":600000,"systemUsec":400000,"periods":50,"throttledPeriods":10,"throttledUsec":900000,"limitCores":null}]}"#;

    fn snap(body: &str) -> Option<ProbeSnapshot> {
        Some(ProbeSnapshot { readings: parse_probe_doc(body).unwrap(), probes: 1, errors: vec![] })
    }

    #[test]
    fn a_probes_answer_is_read_with_its_limits() {
        let r = parse_probe_doc(PROBE_BODY).unwrap();
        assert_eq!(r.len(), 2);
        assert_eq!((r[0].component.as_str(), r[0].pod.as_str(), r[0].usage_usec, r[0].throttled_periods), ("broker", "broker-0", 5_000_000, 3));
        assert_eq!(r[0].limit_cores, Some(3.0));
        assert_eq!(r[1].limit_cores, None, "a null limit is no limit");
        assert!(parse_probe_doc("not json").is_err() && parse_probe_doc("{}").is_err());
    }

    #[test]
    fn an_http_body_is_taken_only_from_a_200() {
        assert_eq!(parse_http_body(b"HTTP/1.0 200 OK\r\nContent-Type: x\r\n\r\n{\"a\":1}").unwrap(), "{\"a\":1}");
        assert!(parse_http_body(b"HTTP/1.0 503 Service Unavailable\r\n\r\nno").unwrap_err().contains("503"));
        assert!(parse_http_body(b"garbage").is_err());
    }

    #[test]
    fn the_cpu_in_a_burst_is_the_difference_between_the_two_snapshots() {
        let after = PROBE_BODY.replace("5000000", "8200000").replace("\"periods\":100", "\"periods\":130").replace("\"throttledPeriods\":10", "\"throttledPeriods\":19");
        let j = probe_delta_json(&snap(PROBE_BODY), &snap(&after));
        let c = j["containers"].as_array().unwrap();
        assert_eq!(c.len(), 2);
        assert_eq!((c[0]["pod"].as_str(), c[0]["usageUsec"].as_u64(), c[0]["periods"].as_u64()), (Some("broker-0"), Some(3_200_000), Some(30)));
        assert_eq!((c[1]["component"].as_str(), c[1]["throttledPeriods"].as_u64(), c[1]["limitCores"].is_null()), (Some("callout"), Some(9), true));
        assert_eq!(j["probes"], 1);
    }

    #[test]
    fn a_restarted_or_missing_container_is_left_out_and_no_probe_is_null() {
        let restarted = PROBE_BODY.replace("5000000", "100");                         // the broker's counters went backwards
        let j = probe_delta_json(&snap(PROBE_BODY), &snap(&restarted));
        assert_eq!(j["containers"].as_array().unwrap().len(), 1);
        let only_broker = r#"{"containers":[{"pod":"broker-0","container":"broker","component":"broker","usageUsec":9000000,"periods":120}]}"#;
        assert_eq!(probe_delta_json(&snap(PROBE_BODY), &snap(only_broker))["containers"].as_array().unwrap().len(), 1);
        assert!(probe_delta_json(&None, &None).is_null() && probe_delta_json(&snap(PROBE_BODY), &None).is_null());
        let empty = Some(ProbeSnapshot { probes: 2, errors: vec!["x: timed out".into()], ..Default::default() });
        let j = probe_delta_json(&snap(PROBE_BODY), &empty);
        assert_eq!(j["containers"].as_array().unwrap().len(), 0);
        assert_eq!(j["errors"][0], "x: timed out");                                  // said, so a missing reading is not mistaken for no CPU use
    }

    /// A real TCP server on loopback answering with a canned HTTP/1.0 response, asked through the same code the generator uses.
    fn serve_once(response: &'static str) -> SocketAddr {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        std::thread::spawn(move || {
            use std::io::{Read, Write};
            for s in l.incoming().take(2) {
                let mut s = s.unwrap();
                let mut buf = [0u8; 256];
                let n = s.read(&mut buf).unwrap();
                assert!(String::from_utf8_lossy(&buf[..n]).starts_with("GET /counters HTTP/1.0"));
                s.write_all(response.as_bytes()).unwrap();
            }
        });
        addr
    }

    #[test]
    fn the_generator_reads_a_probe_over_http_and_a_dead_one_is_an_error_not_a_hang() {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let body: &'static str = Box::leak(format!("HTTP/1.0 200 OK\r\nContent-Type: application/json\r\n\r\n{PROBE_BODY}").into_boxed_str());
        let addr = serve_once(body);
        let snap = rt.block_on(read_probes(&addr.to_string())).unwrap();
        assert_eq!((snap.probes, snap.readings.len(), snap.errors.len()), (1, 2, 0));
        let dead = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();   // nothing listens there any more
        let snap = rt.block_on(read_probes(&dead.to_string())).unwrap();
        assert_eq!((snap.probes, snap.readings.len(), snap.errors.len()), (1, 0, 1));
        assert!(rt.block_on(read_probes("")).is_none(), "unset: not asked");
    }

    #[test]
    fn the_cgroup_cpu_counters_are_read_from_cpu_stat() {
        let s = parse_cpu_stat(CPU_STAT_V2).unwrap();
        assert_eq!(s, CpuStat { usage_usec: 1812754, user_usec: 1118932, system_usec: 693821, nr_periods: 40, nr_throttled: 7, throttled_usec: 350000 });
        assert!(parse_cpu_stat("usage_usec not-a-number\n").is_none() && parse_cpu_stat("").is_none());
        assert!(parse_cpu_stat("nr_periods 3\n").is_none(), "a file without usage_usec is not a cpu.stat");
    }

    #[test]
    fn the_cpu_limit_is_quota_over_period_and_max_is_no_limit() {
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2.0));
        assert_eq!(parse_cpu_max("50000 100000"), Some(0.5));
        assert_eq!(parse_cpu_max("max 100000\n"), None);
        assert_eq!(parse_cpu_max(""), None);
    }

    #[test]
    fn the_burst_report_carries_the_difference_and_null_when_a_reading_failed() {
        let b = CpuStat { usage_usec: 1_000_000, nr_periods: 10, nr_throttled: 1, throttled_usec: 20_000, ..Default::default() };
        let a = CpuStat { usage_usec: 1_900_000, user_usec: 5, system_usec: 3, nr_periods: 16, nr_throttled: 4, throttled_usec: 120_000 };
        let j = cpu_delta_json(Some((b, Some(2.0))), Some((a, Some(2.0))));
        assert_eq!(j["usageUsec"], 900_000);
        assert_eq!((j["periods"].as_u64(), j["throttledPeriods"].as_u64(), j["throttledUsec"].as_u64()), (Some(6), Some(3), Some(100_000)));
        assert_eq!(j["limitCores"], 2.0);
        assert!(cpu_delta_json(None, Some((a, None))).is_null() && cpu_delta_json(Some((b, None)), None).is_null());
        assert!(cpu_delta_json(Some((a, None)), Some((b, None)))["usageUsec"] == 0, "a counter that went backwards is 0, not a huge number");
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

    // -- bad-faith connections ------------------------------------------------------------------------------------------------------

    #[test]
    fn bad_faith_counts_matches_the_configured_fractions_exactly() {
        assert_eq!(bad_faith_counts(1000, [0.0, 0.0, 0.0, 0.0]), [0, 0, 0, 0], "the default: every existing run's math is untouched");
        assert_eq!(bad_faith_counts(1000, [0.1, 0.2, 0.3, 0.0]), [100, 200, 300, 0]);
        assert_eq!(bad_faith_counts(1000, [0.25, 0.25, 0.25, 0.25]), [250, 250, 250, 250], "nothing left for good faith, and that is allowed");
        assert_eq!(bad_faith_counts(10, [0.0, 0.0, 0.0, 1.0]), [0, 0, 0, 10]);
    }

    #[test]
    fn bad_faith_counts_never_exceeds_total_even_when_rounding_would_overshoot() {
        // Four fractions of 0.3 each round to 3 of 9 connections: 12 in all, 3 more than there are. Shrunk from the costliest kind
        // backwards (incomplete-handshake, then forged, then nonexistent-account), one at a time, until it fits.
        let counts = bad_faith_counts(9, [0.3, 0.3, 0.3, 0.3]);
        assert_eq!(counts.iter().sum::<usize>(), 9);
        assert_eq!(counts, [3, 2, 2, 2]);
        // A smaller overshoot (1, not 3) takes only one away, from the costliest kind that has anything to give.
        assert_eq!(bad_faith_counts(3, [0.5, 0.5, 0.0, 0.0]), [2, 1, 0, 0]);
    }

    #[test]
    fn ready_to_snapshot_fires_on_good_faith_alone_not_on_bad_faith_stragglers() {
        // 100 good faith of 150 total (50 bad faith, still outstanding): fires the moment the 100th good-faith record is in, with
        // only 100 of 150 records seen so far -- it must not wait for the rest.
        assert!(!ready_to_snapshot(99, 100, 99, 150));
        assert!(ready_to_snapshot(100, 100, 100, 150));
        assert!(ready_to_snapshot(100, 100, 120, 150), "still true once more (bad-faith) records arrive after it already fired once");
    }

    #[test]
    fn ready_to_snapshot_waits_for_everything_when_there_is_no_good_faith_at_all() {
        // A bad-faith-only calibration burst: there is no earlier point to use, so it falls back to the old meaning, after everything.
        assert!(!ready_to_snapshot(0, 0, 49, 50));
        assert!(ready_to_snapshot(0, 0, 50, 50));
    }

    fn test_ctx() -> Ctx {
        Ctx {
            url: "ws://127.0.0.1:1".into(),
            issuer: issuer_for_tests(),
            rogue_issuer: Issuer::random(&["app-feed-publish-orders".to_string()], 60).unwrap(),
            connector: Connector::Plain,
            timeout: Duration::from_millis(1000),
            metrics: Arc::new(Metrics::new("test".into())),
        }
    }
    fn issuer_for_tests() -> Issuer {
        // The same golden-vector account the ticket:: tests use, not exported from there: construct it the same way main.rs always
        // does, from whatever a real deployment's STORM_SCOPED_SEED/STORM_AUTH_ACCOUNT would be.
        Issuer::new(
            "SAAG4OGNC2HOEDRURUJXZBIP66IQVBGUVT5RUS5HQUOPS7MJTGVLUEPGXE",
            "AB6RUQE7JN26UCECUZRWGQUZOCHDLET5GAYX5MG26XYZLKNUJFOZ7ISB",
            &["app-feed-publish-orders".to_string()],
            3600,
        )
        .unwrap()
    }

    #[test]
    fn kinds_and_credentials_assigns_the_configured_counts_in_the_fixed_order() {
        let c = Cfg {
            connections: 10, runs: 1, pause_ms: 0, ramp_ms: 0, timeout_ms: 1000, hold_ms: 0, threads: 1, label: "t".into(), report: String::new(),
            samples: false, cpuprobe: String::new(), metrics_port: 0, metrics_hold_s: 0, ca_file: String::new(), url: "ws://x".into(),
            scoped_seed: String::new(), auth_account: String::new(), roles: vec![], ticket_ttl_s: 60,
            bad_faith: [0.1, 0.2, 0.3, 0.1], // invalid 1, nonexistent-account 2, forged 3, incomplete-handshake 1, good faith 3
        };
        let ctx = test_ctx();
        let creds = kinds_and_credentials(&c, &ctx, 1_800_000_000).unwrap();
        let kinds: Vec<Kind> = creds.iter().map(|(k, _)| *k).collect();
        assert_eq!(kinds, vec![
            Kind::Invalid,
            Kind::NonexistentAccount, Kind::NonexistentAccount,
            Kind::Forged, Kind::Forged, Kind::Forged,
            Kind::IncompleteHandshake,
            Kind::GoodFaith, Kind::GoodFaith, Kind::GoodFaith,
        ]);
    }

    #[test]
    fn invalid_is_not_a_jwt_at_all() {
        let c_default = |bf: [f64; 4]| Cfg {
            connections: 1, runs: 1, pause_ms: 0, ramp_ms: 0, timeout_ms: 1000, hold_ms: 0, threads: 1, label: "t".into(), report: String::new(),
            samples: false, cpuprobe: String::new(), metrics_port: 0, metrics_hold_s: 0, ca_file: String::new(), url: "ws://x".into(),
            scoped_seed: String::new(), auth_account: String::new(), roles: vec![], ticket_ttl_s: 60, bad_faith: bf,
        };
        let ctx = test_ctx();
        let (kind, cred) = kinds_and_credentials(&c_default([1.0, 0.0, 0.0, 0.0]), &ctx, 1).unwrap().remove(0);
        assert_eq!(kind, Kind::Invalid);
        let Credential::Garbage = cred else { panic!("invalid must be Credential::Garbage") };
    }

    #[test]
    fn incomplete_handshake_sends_nothing() {
        let c_default = |bf: [f64; 4]| Cfg {
            connections: 1, runs: 1, pause_ms: 0, ramp_ms: 0, timeout_ms: 1000, hold_ms: 0, threads: 1, label: "t".into(), report: String::new(),
            samples: false, cpuprobe: String::new(), metrics_port: 0, metrics_hold_s: 0, ca_file: String::new(), url: "ws://x".into(),
            scoped_seed: String::new(), auth_account: String::new(), roles: vec![], ticket_ttl_s: 60, bad_faith: bf,
        };
        let ctx = test_ctx();
        let (kind, cred) = kinds_and_credentials(&c_default([0.0, 0.0, 0.0, 1.0]), &ctx, 1).unwrap().remove(0);
        assert_eq!(kind, Kind::IncompleteHandshake);
        let Credential::NoConnect = cred else { panic!("incomplete-handshake must be Credential::NoConnect") };
    }

    #[test]
    fn forged_presents_a_real_ticket_signed_wrongly_and_nonexistent_account_a_rogue_one_signed_correctly() {
        let c_default = |bf: [f64; 4]| Cfg {
            connections: 1, runs: 1, pause_ms: 0, ramp_ms: 0, timeout_ms: 1000, hold_ms: 0, threads: 1, label: "t".into(), report: String::new(),
            samples: false, cpuprobe: String::new(), metrics_port: 0, metrics_hold_s: 0, ca_file: String::new(), url: "ws://x".into(),
            scoped_seed: String::new(), auth_account: String::new(), roles: vec![], ticket_ttl_s: 60, bad_faith: bf,
        };
        let ctx = test_ctx();

        let (kind, cred) = kinds_and_credentials(&c_default([0.0, 0.0, 1.0, 0.0]), &ctx, 1).unwrap().remove(0);
        assert_eq!(kind, Kind::Forged);
        let Credential::Ticket { ticket, correct_signature } = cred else { panic!("forged must carry a real ticket") };
        assert!(!correct_signature, "forged signs with the wrong key");
        let claims: serde_json::Value = serde_json::from_slice(&data_encoding::BASE64URL_NOPAD.decode(ticket.jwt.split('.').nth(1).unwrap().as_bytes()).unwrap()).unwrap();
        assert_eq!(claims["nats"]["issuer_account"], ctx.issuer.public(), "the real account, not a rogue one: this is a stolen real ticket, not a forged JWT");

        let (kind, cred) = kinds_and_credentials(&c_default([0.0, 1.0, 0.0, 0.0]), &ctx, 1).unwrap().remove(0);
        assert_eq!(kind, Kind::NonexistentAccount);
        let Credential::Ticket { ticket, correct_signature } = cred else { panic!("nonexistent-account must carry a ticket") };
        assert!(correct_signature, "the nonce signature itself is fine: only the account is the problem");
        let claims: serde_json::Value = serde_json::from_slice(&data_encoding::BASE64URL_NOPAD.decode(ticket.jwt.split('.').nth(1).unwrap().as_bytes()).unwrap()).unwrap();
        assert_eq!(claims["nats"]["issuer_account"], ctx.rogue_issuer.public());
        assert_ne!(claims["nats"]["issuer_account"], ctx.issuer.public(), "never the real account");
    }

    #[test]
    fn a_plain_run_with_no_bad_faith_configured_reports_exactly_as_before() {
        // outcome_stats, which both the top-level fields and each badFaith entry are built from, gives the same numbers either way:
        // this is what guarantees a report with nothing configured is unaffected by any of this.
        let admitted = |kind| Rec { kind, outcome: "admitted", detail: String::new(), auth_ms: Some(10.0), total_ms: Some(20.0), open_ms: None, start_ms: 0.0, sent_ms: Some(1.0), done_ms: 11.0 };
        let records = [admitted(Kind::GoodFaith), admitted(Kind::GoodFaith)];
        let refs: Vec<&Rec> = records.iter().collect();
        let (counts, _, auth_s, _, success) = outcome_stats(&refs);
        assert_eq!(counts.get("admitted").copied(), Some(2));
        assert_eq!(auth_s["n"], 2);
        assert_eq!(success, 1.0);
    }
}
