# Copyright (c) 2026 ZyvorAI Labs Private Limited.
# SPDX-License-Identifier: Apache-2.0
# atlas-io-agent: the observe-first storage I/O sensor (crates/atlas-io, docs/IO_EBPF.md). No UI,
# no optional DataBridge connectors, no Ceph/ZFS CLIs — this binary only ever talks HTTP on
# :5111 and (in `live` mode, once a CO-RE loader ships) touches /sys/fs/bpf, so the image stays
# minimal on purpose.
# ---- builder ----
FROM docker.io/library/rust:1.98-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p atlas-io --bin atlas-io-agent

# ---- runtime ----
FROM docker.io/library/debian:bookworm-slim@sha256:3783cc01769c7b2b1b83a5c5ad96c815348e28ed7da68e2e3687004faa906251 AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home /var/lib/atlas-io atlas-io \
    && mkdir -p /var/lib/atlas-io && chown atlas-io:atlas-io /var/lib/atlas-io
COPY --from=builder /build/target/release/atlas-io-agent /usr/local/bin/atlas-io-agent
USER atlas-io
WORKDIR /var/lib/atlas-io
ENV ATLAS_IO_BIND=0.0.0.0:5111 \
    ATLAS_IO_MODE=fake
EXPOSE 5111
ENTRYPOINT ["/usr/local/bin/atlas-io-agent"]
