# SPDX-License-Identifier: Apache-2.0
# Build context: the repository root. Built and started once with podman (it prints its usage error); not yet run against a broker in a container.
# Base images are pinned by digest (the multi-platform index, so every --platform resolves). See images/README.md to update them.
ARG RUST_IMAGE=rust:1-alpine@sha256:8d0e82abfdf9a38071e07cabc5afba4ddbe077631a80e4e742a760c38beadc33
ARG ALPINE_IMAGE=alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
FROM ${RUST_IMAGE} AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
COPY components/loadgen/ ./
RUN cargo build --release --locked

FROM ${ALPINE_IMAGE}
RUN apk add --no-cache ca-certificates
COPY --from=build /src/target/release/auth-storm /usr/local/bin/loadgen
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/loadgen"]
