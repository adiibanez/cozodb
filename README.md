# cozodb

Erlang/BEAM NIF bindings for [CozoDB](https://github.com/cozodb/cozo) using Rustler.

CozoDB is a FOSS embeddable, transactional, relational-graph-vector database, with a Datalog query engine and time travelling capability, perfect as the long-term memory for LLMs and AI.

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
- [Storage Engines](#storage-engines)
- [RocksDB Backends](#rocksdb-backends)
  - [rocksdb (cozorocks)](#1-rocksdb-cozorocks---default)
  - [newrocksdb (rust-rocksdb)](#2-newrocksdb-rust-rocksdb---alternative)
- [Build Options](#build-options)
  - [io_uring](#io_uring)
- [Configuration](#configuration)
  - [jemalloc](#jemalloc)
  - [Docker](#docker)
  - [Memory Tuning Tips](#memory-tuning-tips)
- [Development](#development)
- [License](#license)

## Quick Start

```erlang
%% Open an in-memory database
{ok, Db} = cozodb:open(mem).

%% Run a query
{ok, Result} = cozodb:run(Db, "?[] <- [[1, 2, 3]]").

%% Close the database
%% Notice that close no longer releases the NIF resource. This will be done
%% when the Erlang GC triggers as long as you no longer have a reference to
%% `Db`.
ok = cozodb:close(Db).
```

## Installation

### Requirements

| Platform | Dependencies |
|----------|--------------|
| **Runtime** | Erlang OTP27+ and/or Elixir (latest) |
| **Build** | Rust 1.76.0+, Make |
| **macOS** | `liblz4`, `libssl` |
| **Linux** | `build-essential`, `liblz4-dev`, `libncurses-dev`, `libsnappy-dev`, `libssl-dev`, `pkg-config` |
| **Linux (io_uring)** | All of the above plus `liburing-dev` (see [io_uring](#io_uring)) |

### Erlang

Add the following to your `rebar.config` file:

```erlang
{deps, [
    {cozodb,
      {git, "https://github.com/leapsight/cozodb.git", {branch, "master"}}
    }
]}.
```

### Elixir

Add the following to your `mix.exs` file:

```elixir
defp deps do
  [
    {:cozodb,
      git: "https://github.com/leapsight/cozodb.git",
      branch: "master"
    }
  ]
end
```

## Basic Usage

```erlang
%% In-memory database
{ok, Db} = cozodb:open(mem).

%% SQLite database
{ok, Db} = cozodb:open(sqlite, "/path/to/db.sqlite").

%% RocksDB database (default backend)
{ok, Db} = cozodb:open(rocksdb, "/path/to/db").

%% New RocksDB backend (if compiled with new-rocksdb-default feature)
{ok, Db} = cozodb:open(newrocksdb, "/path/to/db").
```

## Storage Engines

CozoDB supports multiple storage engines:

| Engine | Description |
|--------|-------------|
| `mem` | In-memory storage (no persistence) |
| `sqlite` | SQLite backend (good for small datasets, single-writer) |
| `rocksdb` | RocksDB via cozorocks C++ FFI (default, high-performance) |
| `newrocksdb` | RocksDB via rust-rocksdb crate (comprehensive env var config) |

## RocksDB Backends

There are two RocksDB backends available:

### 1. `rocksdb` (cozorocks) - Default

The default RocksDB backend uses **cozorocks**, a C++ FFI bridge. This is the production-tested backend included in CozoDB upstream.

**Configuration:** Use a RocksDB OPTIONS file placed in the database directory.

### 2. `newrocksdb` (rust-rocksdb) - Alternative

An alternative RocksDB backend using the official **rust-rocksdb** crate. This backend supports comprehensive configuration via environment variables.

> **IMPORTANT:** The two RocksDB backends are **mutually exclusive**. They cannot be compiled together due to allocator conflicts (both link to RocksDB with different memory management configurations). Attempting to use both will cause "pointer being freed was not allocated" crashes.

#### Building with newrocksdb

To use the `newrocksdb` backend, set the `COZODB_BACKEND` environment variable at compile time:

```bash
# Use newrocksdb backend
COZODB_BACKEND=newrocksdb make build

# Or use the convenience target
make build-newrocksdb

# Use default rocksdb backend (cozorocks)
make build
# or explicitly
make build-rocksdb
```

This works the same way when `cozodb` is used as a rebar3/mix dependency:

```bash
COZODB_BACKEND=newrocksdb rebar3 compile
```

#### newrocksdb Environment Variables

The `newrocksdb` backend reads configuration from `COZO_ROCKSDB_*` environment variables:

##### General Options
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_CREATE_IF_MISSING` | bool | true | Create database if it doesn't exist |
| `COZO_ROCKSDB_PARANOID_CHECKS` | bool | false | Enable aggressive data validation |
| `COZO_ROCKSDB_MAX_OPEN_FILES` | i32 | -1 | Maximum open file handles (-1 = unlimited) |
| `COZO_ROCKSDB_MAX_FILE_OPENING_THREADS` | i32 | 16 | Threads for opening files |

##### Parallelism
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_INCREASE_PARALLELISM` | i32 | num_cpus | Total background threads |
| `COZO_ROCKSDB_MAX_BACKGROUND_JOBS` | i32 | 2 | Maximum background jobs |
| `COZO_ROCKSDB_MAX_SUBCOMPACTIONS` | u32 | 1 | Parallel compaction threads |

##### Write Buffer (Memtable)
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_WRITE_BUFFER_SIZE` | usize | 64MB | Size of single memtable |
| `COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER` | i32 | 2 | Maximum number of memtables |
| `COZO_ROCKSDB_MIN_WRITE_BUFFER_NUMBER_TO_MERGE` | i32 | 1 | Minimum memtables to merge |

##### Compaction
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_COMPACTION_STYLE` | string | level | Style: `level`, `universal`, `fifo` |
| `COZO_ROCKSDB_LEVEL0_FILE_NUM_COMPACTION_TRIGGER` | i32 | 4 | L0 files to trigger compaction |
| `COZO_ROCKSDB_LEVEL0_SLOWDOWN_WRITES_TRIGGER` | i32 | 20 | L0 files to slow down writes |
| `COZO_ROCKSDB_LEVEL0_STOP_WRITES_TRIGGER` | i32 | 36 | L0 files to stop writes |
| `COZO_ROCKSDB_MAX_BYTES_FOR_LEVEL_BASE` | u64 | 256MB | Max bytes for level 1 |
| `COZO_ROCKSDB_TARGET_FILE_SIZE_BASE` | u64 | 64MB | Target file size for level 1 |

##### Compression
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_COMPRESSION_TYPE` | string | lz4 | `none`, `snappy`, `zlib`, `lz4`, `lz4hc`, `zstd` |
| `COZO_ROCKSDB_BOTTOMMOST_COMPRESSION_TYPE` | string | zstd | Bottom level compression |

##### Block-Based Table
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_BLOCK_SIZE` | usize | 4KB | Block size in bytes |
| `COZO_ROCKSDB_BLOCK_CACHE_SIZE` | usize | 8MB | Block cache size |
| `COZO_ROCKSDB_BLOOM_FILTER_BITS_PER_KEY` | f64 | 10.0 | Bloom filter bits per key |

##### Blob Storage (BlobDB)
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_ENABLE_BLOB_FILES` | bool | false | Enable blob storage |
| `COZO_ROCKSDB_MIN_BLOB_SIZE` | u64 | 0 | Minimum size to store as blob |
| `COZO_ROCKSDB_BLOB_FILE_SIZE` | u64 | 256MB | Target blob file size |
| `COZO_ROCKSDB_ENABLE_BLOB_GC` | bool | true | Enable blob garbage collection |

##### WAL (Write-Ahead Log)
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_WAL_DIR` | string | db path | WAL directory path |
| `COZO_ROCKSDB_WAL_TTL_SECONDS` | u64 | 0 | WAL file TTL (0 = disabled) |
| `COZO_ROCKSDB_MAX_TOTAL_WAL_SIZE` | u64 | 0 | Max total WAL size (0 = auto) |

##### I/O Options
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_USE_DIRECT_READS` | bool | false | Direct I/O for reads |
| `COZO_ROCKSDB_USE_DIRECT_IO_FOR_FLUSH_AND_COMPACTION` | bool | false | Direct I/O for writes |
| `COZO_ROCKSDB_ALLOW_MMAP_READS` | bool | false | Memory-map file reading |
| `COZO_ROCKSDB_ALLOW_MMAP_WRITES` | bool | false | Memory-map file writing |

##### Statistics
| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZO_ROCKSDB_ENABLE_STATISTICS` | bool | false | Enable statistics collection |

#### Example: newrocksdb with Custom Config

```erlang
%% Set environment variables before opening the database
os:putenv("COZO_ROCKSDB_WRITE_BUFFER_SIZE", "134217728"),  %% 128MB
os:putenv("COZO_ROCKSDB_MAX_WRITE_BUFFER_NUMBER", "4"),
os:putenv("COZO_ROCKSDB_COMPRESSION_TYPE", "zstd"),
os:putenv("COZO_ROCKSDB_ENABLE_STATISTICS", "true"),

{ok, Db} = cozodb:open(newrocksdb, "/path/to/db").
```

## Build Options

All build options are controlled via environment variables and work with `make build`, `rebar3 compile`, and `mix compile`:

| Variable | Values | Default | Description |
|----------|--------|---------|-------------|
| `COZODB_BACKEND` | `rocksdb`, `newrocksdb` | `rocksdb` | RocksDB backend selection |
| `COZODB_IO_URING` | `true`, `false` | `false` | Enable Linux io_uring async I/O |

### io_uring

Linux io_uring support for RocksDB async I/O is available as an opt-in build feature. It is **disabled by default** because the combination of io_uring + jemalloc + RocksDB 10.9.1 causes segfaults in container environments (Docker with named volumes, AWS ECS). This is due to TLS (Thread Local Storage) conflicts between RocksDB's per-thread io_uring instances, jemalloc's thread caches, and the BEAM VM's dirty IO scheduler threads.

When disabled, RocksDB uses standard `pread`/`pwrite` syscalls which perform well across all environments.

To enable io_uring (Linux-only, requires `liburing-dev`):

```bash
# Direct build
COZODB_IO_URING=true make build

# As a rebar3 dep
COZODB_IO_URING=true rebar3 compile

# Docker
docker build --build-arg COZODB_IO_URING=true -t myapp .
```

> **WARNING:** Only enable io_uring for bare-metal Linux deployments where you have verified it works with your kernel version. Do not enable in container environments (Docker, ECS, Kubernetes).

## Configuration

### jemalloc

The NIF uses jemalloc as the global allocator for both Rust and RocksDB (C++) memory. This is enabled by default via the `jemalloc` Cargo feature.

#### Runtime Configuration

Configure jemalloc behavior via environment variables at application startup:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZODB_JEMALLOC_DIRTY_DECAY_MS` | i64 | 1000 | Dirty page decay time in ms |
| `COZODB_JEMALLOC_MUZZY_DECAY_MS` | i64 | 1000 | Muzzy page decay time in ms |
| `COZODB_JEMALLOC_BACKGROUND_THREAD` | bool | true | Enable background purging thread |
| `COZODB_JEMALLOC_NARENAS` | u32 | auto | Number of arenas (optional) |

#### Heap Profiling (opt-in)

Heap profiling via `dump_heap_profile/1` requires the `jemalloc-profiling` Cargo feature, which is **not** enabled by default. This feature adds platform-specific stack unwinding (libunwind on aarch64) that can cause issues in some container environments (e.g., AWS Graviton ECS).

To enable heap profiling, build with:

```bash
cd native/cozodb && cargo build --release --features jemalloc-profiling
```

Then at runtime, set `MALLOC_CONF="prof:true"` before starting the application. Without the feature, `dump_heap_profile/1` returns `{error, "profiling_not_compiled"}`.

### Docker

When running in Docker containers, additional configuration is required.

#### C++20 Support

Build the Rust NIF with C++20 support (required by newer RocksDB headers):

```Dockerfile
ENV CXXFLAGS="-std=c++20"
```

#### jemalloc Page Size

Set page size for jemalloc to match Linux x86_64 (4KB = 2^12). This prevents "page size mismatch" errors when running in Docker containers (different from macOS Apple Silicon which uses 16KB or 64KB pages):

```Dockerfile
ENV JEMALLOC_SYS_WITH_LG_PAGE=12
```

#### jemalloc Container Defaults

For container compatibility, set `JEMALLOC_SYS_WITH_MALLOC_CONF` to bake safe defaults directly into the binary (no runtime `MALLOC_CONF` needed). See the [jemallocator documentation](https://github.com/tikv/jemallocator/blob/master/jemalloc-sys/README.md) for more details.

```Dockerfile
ENV JEMALLOC_SYS_WITH_MALLOC_CONF="background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:1000"
```

#### io_uring in Docker

io_uring is disabled by default, which is the recommended setting for container deployments. If you need to enable it (not recommended), pass the build arg:

```bash
docker build --build-arg COZODB_IO_URING=true -t myapp .
```

### Memory Tuning Tips

| Use Case | Recommendation |
|----------|----------------|
| **Low memory usage** | Set decay values to 0 for aggressive memory return |
| **High throughput** | Use default decay (1000ms) with background thread enabled |
| **Large datasets** | Increase write buffer size and max write buffer number |

## Development

### Building

```bash
# Default build (rocksdb backend, no io_uring)
make build

# With newrocksdb backend
COZODB_BACKEND=newrocksdb make build

# With io_uring (Linux bare-metal only)
COZODB_IO_URING=true make build

# Combine options
COZODB_BACKEND=newrocksdb COZODB_IO_URING=true make build
```

### Running Tests

```bash
make test
```

### Upgrading the cozo Dependency

```bash
cd native/cozodb
cargo update -p cozo
```

## License

Apache License 2.0
