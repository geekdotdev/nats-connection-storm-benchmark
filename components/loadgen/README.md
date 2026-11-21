# loadgen

The connection-storm generator, a Rust crate (binary `auth-storm`). It opens N WebSocket connections to a nats-server in operator mode at once, each presenting its own ticket (below), and times every one from CONNECT to the broker's PONG or `-ERR` ("auth") and from the start of the connection to the PONG ("total"). Its answer to "is the generator the limit?" is in every report: the runtime's own scheduling delay.

```sh
cd components/loadgen && cargo build --release
STORM_URL=wss://broker:8443 STORM_CA_FILE=ca.pem STORM_SCOPED_SEED_FILE=scoped.seed STORM_AUTH_ACCOUNT_FILE=auth.pub \
  STORM_CONNECTIONS=800 STORM_RUNS=5 STORM_JSON=report.json target/release/auth-storm
```

The settings are listed at the top of `src/main.rs`. The scoped seed is never an argument: `STORM_SCOPED_SEED`, or `STORM_SCOPED_SEED_FILE` for a mounted Secret. `wss://` needs `STORM_CA_FILE`. `STORM_JSON=-` prints the JSON report as one stdout line starting `STORM_REPORT_JSON`, which is how a container's report is collected from `kubectl logs`. Each burst's report has `cgroupCpu`, the generator's own CPU use and throttling during the burst (`usageUsec`, `periods`, `throttledPeriods`, `throttledUsec`, `limitCores`), read from this container's cgroup (v2) at the burst's start and end; `null` where it cannot be read. `STORM_SAMPLES=1` adds every connection's timings to the report: `startMs`, `sentMs` (CONNECT out; null if it never got that far) and `doneMs` (answered), in milliseconds since the run began, to a tenth of a millisecond, plus `authMs`, `totalMs` and `openMs`; the driver sets it so that `bench report` can draw the queue. The earlier plain mode (a user and a shared password) and the no-credentials mode were removed with the password callout.

## Tickets

The broker is in operator mode (`components/provision`, and `components/callout`). Connection *i* presents its own **ticket**: a user JWT signed by the AUTH account's scoped signing key, with no permissions of its own, a fresh user key as its subject, `STORM_ROLES` as its tags (default `app-feed-publish-orders,app-feed-subscribe-alarms`) and an `exp` of `STORM_TICKET_TTL_S` seconds (default 3600). It proves it holds the user key by signing the nonce from the broker's INFO. The settings:

| Name | Meaning |
|---|---|
| `STORM_SCOPED_SEED` or `STORM_SCOPED_SEED_FILE` | the scoped signing seed (`SA...`): `scoped.seed` in the Secret `storm-generator-auth` |
| `STORM_AUTH_ACCOUNT` or `STORM_AUTH_ACCOUNT_FILE` | the AUTH account's public key (`A...`): `auth.pub` in the same Secret |
| `STORM_ROLES`, `STORM_TICKET_TTL_S` | as above |

- **Tickets are made before each run's clock starts**, outside the measured interval and before the scheduling-delay ticker starts: the benchmark measures the broker and the callout, not JWT signing here. 1600 tickets take about 40 ms in a release build.
- **The nonce signature cannot be made early** (each connection's nonce arrives in its INFO). It is made before CONNECT is written, so it is inside "total" (start to PONG) and outside "auth" (CONNECT to PONG). One ed25519 signature per connection is the generator's cost in "total"; the scheduling-delay figure is where a limit there would show.
- **The default timeout is 15 s** (`STORM_TIMEOUT_MS`), not 10 s. A ticket the broker refuses by its own rules (for example one with permissions of its own) is refused only after the broker's authorization timeout, 10 s in these manifests, because the broker hands it to the callout of an account that has none and waits. A callout refusal and a ticket from an unknown operator are refused at once.
- The scoped seed is never printed; the startup line names the scoped key's public key and the roles.
- **No nkeys crate or JWT library.** `src/ticket.rs` is a small implementation of the nkey encoding (base32 with a CRC-16) and the JWT v2 format on `ring`, which is already a dependency through rustls. The Go library is the authority: the tests check public keys and ed25519 signatures against vectors it produced (ed25519 is deterministic, so they match byte for byte), the ticket's exact shape (a scoped signer's JWT must have only empty `pub` and `sub`), and `components/callout` has a test that builds this generator and runs it against a real nats-server, over the WebSocket listener, with the callout.

Ported from a generator written for a private stack; the identity-provider login path and everything specific to that stack were removed. Tested by `cargo test` (percentiles, the CONNECT frame, the metrics text, tickets and nkeys) and by `go test` in `components/callout` (`loadgen_e2e_test.go`: 50 connections admitted through a real broker and the callout; a ticket from another operator refused; bad settings refused at startup). It has not been run at 800 or 1600 connections against a broker in operator mode, in a container, or on a cluster.
