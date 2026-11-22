# SPDX-License-Identifier: Apache-2.0
# Builds nats-server from any repository and commit:
#   --build-arg REPO=https://github.com/nats-io/nats-server.git --build-arg REF=<full commit sha>
# and, optionally, one of the patches in patches/ applied on top (--build-arg PATCH=mutex-profile): a benchmark-only change that is
# recorded on the image (label benchmark.patch) and in its tag, so a patched build can never pass for the commit it was built from.
# Base images are pinned by digest (the multi-platform index, so every --platform resolves). See images/README.md to update them.
ARG GO_IMAGE=golang:1.26-alpine@sha256:ecb67f987aa1a57794c91d19ade7ae0209f2f0f5496aaef9e6ae85ddf92d02ab
ARG ALPINE_IMAGE=alpine:3.20@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc
# The build stage always runs on the build host's own architecture and cross-compiles, so building for
# another platform (--platform linux/amd64 on an arm64 Mac) needs no emulation.
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS build
ARG TARGETOS
ARG TARGETARCH
RUN apk add --no-cache git
ARG REPO=https://github.com/nats-io/nats-server.git
ARG REF
ARG PATCH=
COPY patches /patches
RUN test -n "$REF" || { echo "REF build argument is required (a full commit sha)" >&2; exit 1; }
WORKDIR /src
RUN git init -q . && git remote add origin "$REPO" \
 && git fetch -q --depth 1 origin "$REF" && git checkout -q FETCH_HEAD \
 && git rev-parse HEAD > /REVISION
RUN if [ -n "$PATCH" ]; then git apply --verbose "/patches/$PATCH.patch" && echo "$PATCH" > /PATCH; else : > /PATCH; fi
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-w" -o /out/nats-server .

FROM ${ALPINE_IMAGE}
RUN apk add --no-cache ca-certificates
COPY --from=build /out/nats-server /usr/local/bin/nats-server
COPY --from=build /REVISION /REVISION
COPY --from=build /PATCH /PATCH
ARG REPO
ARG REF
ARG PATCH
LABEL org.opencontainers.image.source="$REPO" org.opencontainers.image.revision="$REF" benchmark.patch="$PATCH"
EXPOSE 4222 8222
ENTRYPOINT ["/usr/local/bin/nats-server"]
