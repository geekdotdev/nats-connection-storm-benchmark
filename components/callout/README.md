# callout

A minimal NATS auth-callout responder, a Go module. It does what the in-tree nats-server benchmark's handler does and nothing more: decode the authorization request, sign a user JWT, reply. No database, no identity provider, no network calls, so the benchmark measures the server and not the callout.

**Not a production callout.** It does not verify that a request was signed by the expected server; the connection to the broker is the trust boundary in this test setup.

## Behaviour

- Subscribes with `--workers` (default 16) queue subscribers, group `auth`, on `$SYS.REQ.USER.AUTH`, as the earlier measurements did.
- A client authenticating as `u<N>` with the shared password is placed in account `A<N>` (the user JWT's audience, which is how a callout places a user when the broker is not in operator mode). `N` must be below `--accounts` (default 16). Anything else gets an error response, so the client is refused at once instead of waiting for the broker's timeout.
- A request that cannot be decoded, or has no user nkey, gets no answer and is counted as dropped.
- `GET /healthz` on `--health-addr` (default `:8080`) is 200 once the subscriptions are in place and the broker connection is up.
- Starts before the broker if it has to: the connection retries.

## Commands and secrets

```sh
callout keygen --seed-file PATH   # new issuer account seed (mode 0600, never overwrites); prints the public key only
callout keygen --json             # prints {"seed","public"} on stdout, for a program to capture (the benchmark driver); not for a terminal
callout serve [--url URL] [--workers N] [--accounts N] [--health-addr ADDR]
```

Secrets are never flags (they show in process listings). Each is read from the environment, or from the file named by `<NAME>_FILE` (a mounted Kubernetes Secret):

| Name | Meaning |
|---|---|
| `CALLOUT_SERVICE_USER`, `CALLOUT_SERVICE_PASSWORD` | the callout's own login to the broker, in the `AUTH` account |
| `CALLOUT_ISSUER_SEED` | the account seed whose public key is the broker's `auth_callout.issuer` |
| `CALLOUT_USER_PASSWORD` | the password every benchmark user presents |

The issuer key and passwords are generated at deploy time and kept in a Secret. None is committed.

## The broker it expects

```
accounts {
  SYS {}
  AUTH { users [ {user: "<CALLOUT_SERVICE_USER>", password: "<...>"} ] }
  A0 {}
  A1 {}            # ... up to A<accounts-1>
}
system_account: SYS
authorization {
  timeout: 10s
  auth_callout {
    issuer: "<public key printed by keygen>"
    account: AUTH
    auth_users: [ "<CALLOUT_SERVICE_USER>" ]
  }
}
```

## Tests

```sh
cd components/callout && go test -race ./...
```

The unit tests decode and check the signed responses; the end-to-end test starts a real nats-server (the library, test only) configured as above and checks who is admitted, into which account, and that a refusal is an error response and not a timeout.
