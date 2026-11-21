# loadgen

The connection-storm generator, a Rust crate (binary `auth-storm`). It opens N WebSocket connections to a nats-server at once, each presenting a user and password, and times every one from CONNECT to the broker's PONG or `-ERR` ("auth") and from the start of the connection to the PONG ("total"). Its answer to "is the generator the limit?" is in every report: the runtime's own scheduling delay.

```sh
cd components/loadgen && cargo build --release
STORM_URL=wss://broker:8222 STORM_CA_FILE=ca.pem STORM_PASSWORD_FILE=user.pw \
  STORM_ACCOUNTS=16 STORM_CONNECTIONS=800 STORM_RUNS=5 STORM_JSON=report.json target/release/auth-storm
```

The settings are listed at the top of `src/main.rs`. In plain mode (the default) connection *i* is the user `u<i mod STORM_ACCOUNTS>`, which is what `components/callout` expects. The password is never an argument: `STORM_PASSWORD`, or `STORM_PASSWORD_FILE` for a mounted Secret. `STORM_AUTH=none` sends no credentials, to measure the generator itself against a broker with no auth. `wss://` needs `STORM_CA_FILE`. `STORM_JSON=-` prints the JSON report as one stdout line starting `STORM_REPORT_JSON`, which is how a container's report is collected from `kubectl logs`.

Ported from a generator written for a private stack; the identity-provider login path and everything specific to that stack were removed. Tested by `cargo test` (percentiles, the CONNECT frame, the metrics text) and by running it against a real nats-server 2.15.3 with `components/callout`, over `ws://` and over `wss://` with a throwaway CA. There is no automated end-to-end test in this repository yet.
