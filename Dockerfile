# syntax=docker/dockerfile:1
#
# One build stage, one small runtime image per binary:
#   docker build --target collector .   (also: uploader, logwatch)
# Separate images let the uploader or logwatch be redeployed without
# recreating the collector.

FROM rust:1-trixie AS build
WORKDIR /src

# Dependencies first, against placeholder sources, so this layer is reused
# until Cargo.toml / Cargo.lock change.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src/bin \
 && echo 'fn main() {}' > src/main.rs \
 && cp src/main.rs src/bin/uploader.rs \
 && cp src/main.rs src/bin/logwatch.rs \
 && cargo build --release --locked \
 && rm -rf src

COPY src ./src
# touch: the real sources can be older than the placeholder build, and cargo
# decides what to rebuild by mtime.
RUN touch src/main.rs src/bin/*.rs \
 && cargo build --release --locked

# The binaries need only glibc + libgcc (TLS is rustls) and CA certificates,
# which is what distroless/cc provides. No shell, no package manager.
# They run as 1000:1000 by default; compose sets `user:` explicitly anyway.

FROM gcr.io/distroless/cc-debian13 AS uploader
WORKDIR /app
COPY --from=build /src/target/release/uploader /usr/local/bin/uploader
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/uploader"]

# Needs the Docker socket, so compose runs it as root.
FROM gcr.io/distroless/cc-debian13 AS logwatch
WORKDIR /app
COPY --from=build /src/target/release/logwatch /usr/local/bin/logwatch
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/logwatch"]

# Writes to ./data relative to the working dir, i.e. /app/data (mount it).
FROM gcr.io/distroless/cc-debian13 AS collector
WORKDIR /app
COPY --from=build /src/target/release/quant_2 /usr/local/bin/quant_2
USER 1000:1000
ENTRYPOINT ["/usr/local/bin/quant_2"]
