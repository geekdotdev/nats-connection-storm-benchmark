# Components

Source for the three things that run in the benchmark. Each is built into an image from `images/`.

| Directory | Language | What it is |
|---|---|---|
| `loadgen/` | Rust | the connection-storm generator |
| `callout/` | Go | the auth-callout responder |

nats-server itself is not vendored here: its image is built from a repository and commit given at build time (`images/nats-server`).
