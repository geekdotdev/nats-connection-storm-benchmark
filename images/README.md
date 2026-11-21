# Images

One directory per image, each with its own Dockerfile. Build them with `uv run bench images build <sha>`, not by hand, so the tags and build arguments stay consistent.

| Image | Built from | Build context |
|---|---|---|
| `nats-server` | a git repository and commit, given as build arguments `REPO` and `REF` | `images/nats-server` (empty: the source is cloned inside the build) |
| `callout` | `components/callout` | repository root |
| `loadgen` | `components/loadgen` | repository root |
| `provision` | `components/provision` | repository root (built by `bench images build`, tagged like the callout; the Dockerfile has been built by no one yet) |
| `cpuprobe` | `components/cpuprobe` | repository root (the DaemonSet of `deploy/monitoring`; Go, standard library only, tagged like the callout) |

## Platforms and publishing

```bash
uv run bench images build <sha> --platform linux/amd64,linux/arm64   # one manifest list per image (podman)
uv run bench images build <sha> --platform linux/amd64               # one platform (docker or podman)
uv run bench images build <sha> --ref-name feature/my-branch        # also label and tag the broker image with a branch name
uv run bench images push nats-server:<first 12 of the sha>           # needs REGISTRY, REGISTRY_USER, REGISTRY_TOKEN
```

- The commit is the identity: the broker image is always tagged `nats-server:<first 12 of the sha>`, labelled `org.opencontainers.image.revision` (full sha) and `org.opencontainers.image.source` (the repository), and reports the sha in `/REVISION`.
- `--ref-name` adds the label `org.opencontainers.image.ref.name` (the name as given) and a second tag with the unsupported characters turned into `-` (`feature/my-branch` becomes `feature-my-branch`). It records where the branch pointed when the image was built; the branch moves and the sha does not. Push the second tag by naming it: `bench images push nats-server:<sha12> nats-server:feature-my-branch` (both tags end up on the same digest). Names that would look like a short sha are refused.
- A multi-platform build starts from an empty tag: it removes the old list or image first (`podman manifest rm` for a list; `rmi` fails on one), because podman would otherwise append to the old list.

- No `--platform` builds for this machine. EKS nodes are usually x86 and an Apple Silicon Mac builds arm64, so build both for anything you push. The Go images cross-compile on the build machine; the Rust load generator is built under emulation for the other architecture (about a minute).
- The tag does not say which platform an image is, and a build for one platform replaces the same tag built for another. Do not `kind load` an image built for the other architecture: the pod fails with `exec format error`. Rebuild without `--platform` for local runs.
- `push` prints each pushed image by digest (`ghcr.io/<account>/nats-server@sha256:...`). Deploy that, not the tag.
- GHCR names must be lowercase, and a package is private until you make it public in GitHub (package settings, visibility). EKS can pull it without credentials only once it is public.
- `--insecure` skips TLS verification and works only for a registry on localhost, for trying a push without publishing: run `registry:2` and use `REGISTRY=127.0.0.1:5000/test` (not `localhost`, which is IPv6 first and hits AirPlay on a Mac).

Tag the broker image with the commit it was built from, and use the image digest, not the tag, when deploying, so a tag can never change under a run. The `nats-server`, `callout` and `loadgen` Dockerfiles were built with podman for both platforms before operator mode, and the broker image started (`nats-server --version`, and the commit it reports in `/REVISION`). The callout and load generator were rebuilt after operator mode replaced the password callout, and the `provision` image was built for the first time, on 2026-11-21 with podman for arm64 only (one platform, with the `:dev` tags); all three then ran on a kind cluster against the broker image (`deploy/README.md`). The amd64 and arm64 builds that were pushed have not been run.

`callout`, `loadgen` and `provision` are tagged `<name>:src-<12 hex>`, a hash of their source files and Dockerfile (not of a commit, since the source may be uncommitted); a single-platform build also gets `<name>:dev` for the local flow. `bench images build --only callout,loadgen,provision` builds just those, with no commit needed. The `callout` (`src-faac5bdfebd5`) and `loadgen` (`src-9a52195fe7db`) images published to GHCR on 2026-11-18 hold the earlier password callout and plain generator: **they are superseded**. The images built from the operator-mode source were pushed as amd64 and arm64 lists on 2026-11-21 and the EKS overlays pin them: `ghcr.io/cfedersp/callout@sha256:21ca37a0…` (`src-3cff67e67dd2`), `ghcr.io/cfedersp/loadgen@sha256:93d0fc35…` (`src-16759d90c2a9`, pushed later the same day: it records each burst's CPU use and throttling as well as every connection's timings; the earlier lists `src-f92df7b220d0`, timings only, and `src-d4e091f900ab`, neither, are superseded) and `ghcr.io/cfedersp/provision@sha256:b01b5bc5…` (`src-c1a3acc650f6`); the `provision` package was new and private, and was made public the same day.

## Broker images for the lock and multiplexing comparison

Published to `ghcr.io/cfedersp/nats-server` on 2026-11-21 as amd64 and arm64 lists (tagged by commit and by branch name, pinned by digest in `lock-variants.toml`, which names each digest's source). All were built with `bench images build <sha> --only nats-server --repo https://github.com/cfedersp/nats-server.git --platform linux/amd64,linux/arm64 --ref-name <branch>`; the repository is given because the commits are on that fork, not on nats-io:

| Variant | Branch | Commit | Digest |
|---|---|---|---|
| `main` | upstream main (read lock, #8905) | `582fb8e939a7` | `sha256:7e89e5a9ad5a…` |
| `nolock` | `feature/lockfree-acc-conns-update` | `37260075e531` | `sha256:4214ee07b2bb…` |
| `mux_main` | `feature/auth-callout-mux-on-main` | `ec9244c3444d` | `sha256:0676f13113d5…` |
| `mux_nolock` | `feature/auth-callout-mux-on-lockfree` | `7ea787e2b5ee` | `sha256:d35b480b8d13…` |

Each resolves anonymously with both platforms. The `mux` change is an experiment set aside on 2026-11-16 (auth callout reply multiplexing: one shared reply subscription per account instead of one per connection) and applied to the two bases; it was checked with `go test -race -run TestAuthCallout` and not with the full nats-server suite. None of these images has been run in the benchmark yet.

## Base images are pinned by digest

Each Dockerfile names its base images as `name:tag@sha256:<index digest>` (`ARG GO_IMAGE` / `RUST_IMAGE` / `ALPINE_IMAGE`): `golang:1.26-alpine`, `rust:1-alpine` and `alpine:3.20`, pinned 2026-11-18. The digest is the multi-platform index, so `--platform linux/amd64,linux/arm64` still resolves each platform; the tag stays for the reader, the digest is what is pulled. `tests/test_preflight.py` fails if a base image is not pinned.

To update a pin, read the index digest from the registry and replace it in the four Dockerfiles (the content tags of `callout`, `loadgen` and `provision` change with it, because the Dockerfile is part of what they hash):

```sh
tok=$(curl -s "https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/alpine:pull" | python3 -c "import json,sys; print(json.load(sys.stdin)['token'])")
curl -sI -H "Authorization: Bearer $tok" -H "Accept: application/vnd.oci.image.index.v1+json" https://registry-1.docker.io/v2/library/alpine/manifests/3.20 | grep -i docker-content-digest
```

On 2026-11-18 `callout`, `loadgen` and the `nats-server` list for `582fb8e939a7` on GHCR were rebuilt and pushed from the pinned Dockerfiles (the first two since superseded, above). The `nats-server` rebuild came out with the same digest as the earlier unpinned build (`sha256:7e89e5a9…`): it ran with most layers served from the local build cache, so this shows the pins resolve to the same bases and the same output, not an independent from-scratch reproduction (that would need `podman build --no-cache`).
