# provision

Makes the operator, account and user material the benchmark's broker, auth callout and load generator share, and stores it as Kubernetes objects. A Go module. It runs once, as an init container of the callout pod.

**Not a production tool.** It exists so a throwaway lab can start an operator-mode broker with no `nsc` binary and no manual steps. Keys live in Kubernetes Secrets, so anything that can read those Secrets can sign as that component.

## What it makes

One run makes a complete, self-consistent set and checks it with the jwt library's own validators before storing anything:

- an operator, a SYS account, an AUTH account and one tenant account (JWTs signed by the operator key);
- on the AUTH account: the auth callout turned on (`auth_users` is the callout's own user only, `allowed_accounts` is the tenant account only), a **scoped signing key** whose template denies publish and subscribe on `>` (the load generator signs its tickets with it), and a plain signing key the callout signs its response claim with;
- a plain tenant signing key the callout signs the user JWT it mints with;
- the callout's own login (a user JWT in AUTH and its seed, in `.creds` format).

The operator, SYS, AUTH and tenant identity seeds sign and are then wiped: they are in no object, so the accounts cannot be edited afterwards. A test checks that.

## Objects

| Object | Holds | Read by |
|---|---|---|
| Secret `storm-callout-auth` | `callout.creds`, `auth-signing.seed`, `tenant-signing.seed`, `auth.pub`, `tenant.pub` | the callout only |
| Secret `storm-generator-auth` | `scoped.seed`, `scoped.pub`, `auth.pub` | the load generator only |
| ConfigMap `storm-operator` | `operator.jwt`, `sys.jwt`, `auth.jwt`, `tenant.jwt`, `operator.conf`, `resolver-memory.conf` | the broker (no seeds) |

`operator.conf` names the operator JWT (under `--mount-dir`) and the system account. `resolver-memory.conf` is the MEMORY resolver with the three account JWTs preloaded; a later resolver replaces that one file. Include both from the broker's configuration with a path relative to the main config (nats-server joins an include path to the including file's directory, even an absolute one): with the main config in `/etc/nats` and this ConfigMap mounted at `/etc/storm/operator`, that is `include "../storm/operator/operator.conf"`.

## Behaviour

- The ConfigMap is created last and is the marker that a run finished. If it exists, the program does nothing, so restarting the callout pod (which the benchmark driver does at every variant start) keeps the same keys.
- It never updates or deletes. If some objects exist but the ConfigMap does not (an earlier run stopped part way), it waits `--wait` (default 30s) for another run to finish, then fails with a message naming the three objects to delete.
- It needs a ServiceAccount allowed to `create` and `get` those three objects, and nothing else.

## Commands

```sh
provision [--mount-dir /etc/storm/operator] [--sa-dir DIR] [--wait 30s]   # create the objects in the cluster
provision --out-dir DIR                                                    # write them as files under DIR (must exist), to look at them; refuses to overwrite
```

The cluster client is a few calls over HTTPS with the ServiceAccount token (read again on each request), using the standard library rather than client-go.

## Tests

`go test ./...` (no network, no cluster): validation of the generated JWTs, which seed goes to which object, the create-once behaviour against a fake store, and an end-to-end test that starts a real `nats-server` in operator mode from the files this writes (the MEMORY resolver, the included fragments), logs a minimal responder in with the callout's creds, and sends tickets through it.

## Verified, and not

On a kind cluster (2026-11-21, see `deploy/README.md`): the Kubernetes client against a real API server (the objects are created, and a rerun finds the ConfigMap and does nothing); the ServiceAccount token reaching only this init container; the container image, built with podman for arm64. A failure of this init container (its permission removed on purpose) shows in the pod's status through `terminationMessagePolicy: FallbackToLogsOnError`.

Not verified: two callout pods starting at the same moment (the "wait for the other run" path is tested only against a fake store); a run that stopped part way and was found later (also fake store only); amd64.
