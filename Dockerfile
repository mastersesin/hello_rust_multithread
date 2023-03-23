# Cache Layer
FROM lukemathwalker/cargo-chef:latest-rust-1.68.0 AS chef
WORKDIR app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt update
RUN apt -y install fuse libfuse-dev pkg-config
COPY --from=planner /app/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN cargo chef cook --recipe-path recipe.json
# Build application
COPY . .
RUN cargo build --bin untitled

# Main Build Layer
FROM ubuntu AS runtime
RUN apt update
RUN apt -y install fuse3 libfuse-dev pkg-config openssl libssl-dev wget
RUN wget http://nz2.archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2.17_amd64.deb
RUN dpkg -i libssl1.1_1.1.1f-1ubuntu2.17_amd64.deb
WORKDIR /app
RUN mkdir target
RUN mkdir mount
COPY tmp/test.txt target
COPY --from=builder /app/target/debug/untitled /app

ENTRYPOINT ["/app/untitled"]
