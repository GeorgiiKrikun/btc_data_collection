FROM rust:1-bookworm
ARG UID=1000
ARG GID=100

USER $UID:$GID

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

CMD ["./target/release/quant_2"]
