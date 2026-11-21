# callout

A minimal NATS auth-callout responder, a Go module. It does what the in-tree nats-server benchmark's handler does and nothing more: decode the authorization request, sign a user JWT, reply. No database, no identity provider, no network calls, so the benchmark measures the server and not the callout.

**Not a production callout.** It does not verify that a request was signed by the expected server; the connection to the broker is the trust boundary in this test setup.

## Behaviour

- Subscribes with `--workers` (default 16) queue subscribers, group `auth`, on `$SYS.REQ.USER.AUTH`.
- A request that cannot be decoded, or has no user nkey, gets no answer and is counted as dropped.
- `GET /healthz` on `--health-addr` (default `:8080`) is 200 once the subscriptions are in place and the broker connection is up.
- Starts before the broker if it has to: the connection retries.

## What it does

The callout answers for a broker that trusts an operator (see `components/provision` for where the keys come from, and `docs/operator-mode-plan.md`). It is the only mode: the earlier password mode (users `u<N>` and one shared password, accounts written in the broker's configuration, `callout keygen`) was removed.

- A client presents a **ticket**: a user JWT signed by the AUTH account's scoped signing key, with no permissions of its own and its role names as tags. The broker has already verified it and hands it to the callout in the authorization request.
- The callout maps the role names to subjects (the role-name mapper, `rolenames.go`) and answers with a user JWT for the tenant account, signed by the tenant signing key (`issuer_account` is the tenant account) and valid for `--ttl` (default 1 hour; the broker disconnects the client when it expires), inside a response claim signed by the AUTH signing key. No `nbf` is set.
- **Role names** carry the whole decision: `app-feed-publish-<stem>` grants publish on `app.<stem>` and `app-feed-subscribe-<stem>` grants subscribe, each dash in the stem becoming a dot (`app-feed-subscribe-alarms-high` is `app.alarms.high`). A stem may only be lowercase letters and digits between dashes, so no role can grant a wildcard or more than one exact subject. Anything else is ignored. A direction with no subjects is denied outright (`deny: ">"`), because an empty allow list restricts nothing in NATS. This is a generic re-implementation of the application-feed family of such a naming convention; it has no sensor or form families and no realm-role table.
- **Not checked:** that the ticket's roles were granted by anyone. Whoever holds the scoped key chooses them. A ticket that cannot be decoded gets an error response, so the client is refused at once. A request that cannot be decoded at all gets no answer and is counted as dropped.
- It does less per connection than a callout that verifies tokens, seals responses or looks grants up, so numbers from it describe the broker and the callout round trip, not such a callout.

Secrets, each read from the environment or from the file named by `<NAME>_FILE`:

| Name | Meaning |
|---|---|
| `CALLOUT_CREDS` | the callout's own login: a user JWT and seed in `.creds` format |
| `CALLOUT_AUTH_SIGNING_SEED` | the AUTH account's signing key, which signs the response claim |
| `CALLOUT_TENANT_SIGNING_SEED` | the tenant account's signing key, which signs the user JWT it mints |
| `CALLOUT_TENANT_PUBLIC` | the tenant account's public key (not secret) |

All four are checked at startup, so a bad value fails at once instead of at the first connection attempt.

## Commands

```sh
callout serve [--url URL] [--workers N] [--ttl DURATION] [--health-addr ADDR]
```

Secrets are never flags (they show in process listings): the four above are read from the environment or from `<NAME>_FILE`. They are made by `components/provision` at deploy time and kept in the Secret `storm-callout-auth`. None is committed.

## The broker it expects

A broker in operator mode: `operator`, `system_account` and an account resolver with the SYS, AUTH and tenant account JWTs, all written by `components/provision` (`deploy/stack/base/broker-config.yaml` includes them). The AUTH account's JWT turns the callout on: its `auth_users` is the callout's own user, its `allowed_accounts` is the tenant account, and its scoped signing key is the one tickets are signed with.

## Tests

```sh
cd components/callout && go test -race ./...
```

The unit tests check the role-name mapper (no stem can widen a grant) and the signed responses (issuer, audience, expiry, permissions, a direction with no subjects denied outright, refusals as error responses). The end-to-end tests build `components/provision` and run it, start a real nats-server (the library, test only) from its files and this callout against it, and check the account, what each role allows and refuses, that the broker closes the connection when the minted JWT expires, and startup validation of the secrets. `loadgen_e2e_test.go` also builds `components/loadgen` (offline; skipped without `cargo`) and runs it against that broker and this callout.
