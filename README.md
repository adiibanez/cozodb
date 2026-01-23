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
ok = cozodb:close(Db).
```

## Installation

### Requirements

| Platform | Dependencies |
|----------|--------------|
| **Runtime** | Erlang OTP26+ and/or Elixir (latest) |
| **Build** | Rust 1.76.0+ |
| **macOS** | `liblz4`, `libssl` |
| **Linux** | `build-essential`, `liblz4-dev`, `libncurses-dev`, `libsnappy-dev`, `libssl-dev`, `liburing-dev`, `liburing2`, `pkg-config` |

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

To use the `newrocksdb` backend, you must build with the `new-rocksdb-default` feature instead of the default features.

> **Note:** The `rebar3_cargo` plugin does not support passing Cargo feature flags. Use the Makefile targets or manual Cargo build as described below.

##### Option 1: Makefile with Environment Variable (Recommended)

Use the provided Makefile with the `COZODB_BACKEND` environment variable:

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

##### Option 2: Manual Cargo Build

Build the NIF manually before running rebar3:

```bash
# In native/cozodb directory
cargo build --release --no-default-features --features "new-rocksdb-default"

# Copy the built library to priv/
mkdir -p ../../priv/crates/cozodb
cp target/release/libcozodb.dylib ../../priv/crates/cozodb/cozodb.so  # macOS
# or: cp target/release/libcozodb.so ../../priv/crates/cozodb/cozodb.so  # Linux

# Then compile Erlang code only
cd ../..
rebar3 compile
```

##### Option 3: Modify Cargo.toml

Edit `native/cozodb/Cargo.toml` to change the default features permanently:

```toml
[features]
default = [
    "cozo/storage-sqlite",
    "cozo/storage-new-rocksdb",  # Changed from storage-rocksdb
    "cozo/graph-algo",
    "jemalloc"
]
```

Then build normally with `make build` or `rebar3 compile`.

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

## Configuration

### jemalloc

The NIF uses jemalloc for memory management. Configure via environment variables:

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `COZODB_JEMALLOC_DIRTY_DECAY_MS` | i64 | 1000 | Dirty page decay time in ms |
| `COZODB_JEMALLOC_MUZZY_DECAY_MS` | i64 | 1000 | Muzzy page decay time in ms |
| `COZODB_JEMALLOC_BACKGROUND_THREAD` | bool | true | Enable background purging thread |
| `COZODB_JEMALLOC_NARENAS` | u32 | auto | Number of arenas (optional) |

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

### Memory Tuning Tips

| Use Case | Recommendation |
|----------|----------------|
| **Low memory usage** | Set decay values to 0 for aggressive memory return |
| **High throughput** | Use default decay (1000ms) with background thread enabled |
| **Large datasets** | Increase write buffer size and max write buffer number |

## Development

### Upgrading the cozo Dependency

```bash
cd native/cozodb
cargo update -p cozo
```

### Using newrocksdb Backend as a Consumer

By default, cozodb compiles with the `rocksdb` (cozorocks) backend. If you need the `newrocksdb` backend with environment variable configuration in your own project, you have two options:

#### Option A: Fork and Modify (Recommended)

1. Fork the cozodb repository
2. Modify `native/cozodb/Cargo.toml` to use `new-rocksdb-default` as the default features
3. Point your dependency to your fork

#### Option B: Pre-build the NIF

Before running `rebar3 compile` in your project:

```bash
# Clone cozodb
git clone https://github.com/leapsight/cozodb.git /tmp/cozodb
cd /tmp/cozodb

# Build with newrocksdb backend
COZODB_BACKEND=newrocksdb make cargo-build

# Copy the built NIF to your project's _build directory
# (after rebar3 has fetched dependencies)
mkdir -p YOUR_PROJECT/_build/default/lib/cozodb/priv/crates/cozodb
cp priv/crates/cozodb/cozodb.so YOUR_PROJECT/_build/default/lib/cozodb/priv/crates/cozodb/
```

## License

Apache License 2.0
