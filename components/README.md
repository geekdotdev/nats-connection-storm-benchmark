# Components

Source for the things that run in the benchmark. Each is built into an image from `images/`.

| Directory | Language | What it is |
|---|---|---|
| `loadgen/` | Rust | the connection-storm generator |
| `callout/` | Go | the auth-callout responder |
| `provision/` | Go | makes the operator, account and user keys and stores them as Kubernetes objects (an init container of the callout pod) |

nats-server itself is not vendored here: its image is built from a repository and commit given at build time (`images/nats-server`).
