# Dockerfile for testing cozodb benchmark in Docker
# Tests that jemalloc's disable_initial_exec_tls fix resolves TLS block errors

FROM elixir:1.18-otp-27

# Build args for compile-time feature selection
# Set COZODB_IO_URING=true to enable io_uring (not recommended for containers)
ARG COZODB_IO_URING=false

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    clang \
    libclang-dev \
    pkg-config \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Conditionally install liburing-dev when io_uring is enabled
RUN if [ "$COZODB_IO_URING" = "true" ]; then \
        apt-get update && apt-get install -y liburing-dev && rm -rf /var/lib/apt/lists/*; \
    fi

# Install Rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy the entire cozodb project
COPY . .

# Build the Rust NIF with C++20 support (required by newer RocksDB headers)
ENV CXXFLAGS="-std=c++20"

# CRITICAL: Compile-time jemalloc configuration for container compatibility
# This bakes safe defaults directly into the binary - no runtime MALLOC_CONF needed
# See: https://github.com/tikv/jemallocator/blob/master/jemalloc-sys/README.md
ENV JEMALLOC_SYS_WITH_MALLOC_CONF="background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:1000"

# CRITICAL: Set page size for jemalloc to match Linux x86_64 (4KB = 2^12)
# This prevents "page size mismatch" errors when running in Docker containers
# Different from macOS Apple Silicon which uses 16KB (LG_PAGE=14) or 64KB pages
ENV JEMALLOC_SYS_WITH_LG_PAGE=12

RUN COZODB_IO_URING=${COZODB_IO_URING} make cargo-build

# Copy the built NIF to priv directory
RUN mkdir -p priv/crates/cozodb && \
    cp native/cozodb/target/release/libcozodb.so priv/crates/cozodb/cozodb.so

# Build the Erlang application
RUN mix local.hex --force && \
    mix local.rebar --force

# Compile the main cozodb Erlang project
RUN mix deps.get && mix compile || true
RUN rebar3 compile

# Build the benchmark
WORKDIR /app/benchmark/cozodb_benchmark
RUN mix deps.get && mix compile

# Create directory for benchmark data
RUN mkdir -p /tmp/cozodb_benchmark /app/benchmark_reports

# Environment variables for jemalloc
ENV COZODB_JEMALLOC_DIRTY_DECAY_MS=1000
ENV COZODB_JEMALLOC_MUZZY_DECAY_MS=1000
ENV COZODB_JEMALLOC_BACKGROUND_THREAD=true

# Default command: run a quick benchmark to verify everything works
CMD ["mix", "run", "lib/benchmark_runner.exs", \
     "duration=30", \
     "rampdown=10", \
     "workers=10", \
     "tables=2", \
     "rows=1000", \
     "value_size=256", \
     "workload=mixed_50_50", \
     "engine=rocksdb", \
     "db_path=/tmp/cozodb_benchmark/test_db", \
     "report_dir=/app/benchmark_reports"]
