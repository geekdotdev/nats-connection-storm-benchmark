# SPDX-License-Identifier: Apache-2.0
# Build context: the repository root. Built and smoke-run once with podman (keygen and usage only); not yet run against a broker in a container.
# Base images are pinned by digest (the multi-platform index, so every --platform resolves). See images/README.md to update them.
ARG GO_IMAGE=golang:1.26-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab
ARG ALPINE_IMAGE=alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
# Runs on the build host's architecture and cross-compiles (no emulation for --platform).
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY components/callout/ ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-w" -o /out/callout ./...

FROM ${ALPINE_IMAGE}
COPY --from=build /out/callout /usr/local/bin/callout
USER 65534:65534
ENTRYPOINT ["/usr/local/bin/callout"]
