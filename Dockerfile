FROM rust:1.89 as builder
WORKDIR /build
COPY . .
RUN cargo build --release && cp ./settings.toml ./target/release/

FROM debian:trixie-slim
WORKDIR /app
RUN apt-get update && apt-get install libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/batch_proxy ./
COPY --from=builder /build/target/release/settings.toml ./
CMD ["/app/batch_proxy"]
