# CozoDB Sustained Load Benchmark

A comprehensive benchmarking tool for testing CozoDB concurrency, performance, and stability under sustained load. This tool helps identify issues like `Resource Busy` errors, memory growth, and performance degradation over time.

## Features

- **Configurable duration** - Run benchmarks from seconds to hours
- **Multiple workload patterns** - 100% read, 100% write, 50/50, 80/20 write/read
- **Concurrent workers** - Simulate high concurrency with configurable worker count
- **Large data generation** - Generate GBs of data to simulate realistic scenarios
- **Error tracking** - Records and categorizes errors (Resource Busy, database locked, etc.)
- **Time-series metrics** - Tracks throughput, latency, memory, and errors over time
- **HTML reports** - Interactive charts with Chart.js visualization
- **JSON export** - Raw metrics data for further analysis

## Prerequisites

- Elixir 1.15+
- Erlang/OTP 27+
- CozoDB (parent project)

## Installation

```bash
cd benchmark/cozodb_benchmark
mix deps.get
```

## Quick Start

```bash
# Run with defaults (10 minutes, 100 workers, RocksDB, 128 dirty IO schedulers)
./run_benchmark.sh

# Run a quick 1-minute test
./run_benchmark.sh --duration 60

# Show all options
./run_benchmark.sh --help
```

## Usage

### Using the Shell Script (Recommended)

The `run_benchmark.sh` script provides a convenient way to run benchmarks with all configurable options:

```bash
./run_benchmark.sh [OPTIONS]
```

#### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--dirty-io-schedulers` | 128 | Number of Erlang dirty IO scheduler threads (`+SDio`). This controls how many concurrent NIF calls CozoDB can handle. |
| `--duration` | 600 | Main load phase duration in seconds (600 = 10 minutes) |
| `--rampdown` | 60 | Ramp-down phase duration in seconds. Workers are gradually stopped during this phase to help detect memory leaks. |
| `--workers` | 100 | Number of concurrent worker processes |
| `--tables` | 10 | Number of database tables to create |
| `--rows` | 10000 | Number of rows to seed per table |
| `--value-size` | 1024 | Size of the data field in bytes (for generating large data) |
| `--workload` | mixed_50_50 | Workload pattern (see below) |
| `--engine` | rocksdb | Database engine: `rocksdb`, `sqlite`, or `mem` |
| `--db-path` | /tmp/cozodb_benchmark_sustained | Database storage path |
| `--report-dir` | ./benchmark_reports | Output directory for reports |

#### Workload Patterns

| Pattern | Description |
|---------|-------------|
| `read_only` | 100% read operations |
| `write_only` | 100% write operations |
| `mixed_50_50` | 50% reads, 50% writes on different tables |
| `mixed_80_20_wr` | 80% writes, 20% reads on different tables |

### Examples

#### Basic 10-minute benchmark
```bash
./run_benchmark.sh
```

#### Quick stress test (1 minute, high concurrency)
```bash
./run_benchmark.sh --duration 60 --workers 200
```

#### Test with different dirty IO scheduler counts
```bash
# Test with 64 schedulers
./run_benchmark.sh --dirty-io-schedulers 64 --duration 300

# Test with 256 schedulers
./run_benchmark.sh --dirty-io-schedulers 256 --duration 300
```

#### Heavy write workload
```bash
./run_benchmark.sh --workload mixed_80_20_wr --workers 150 --duration 600
```

#### Generate large amounts of data (stress memory)
```bash
./run_benchmark.sh \
  --rows 100000 \
  --value-size 8192 \
  --duration 1800 \
  --workers 100
```

This creates 10 tables × 100,000 rows × ~8KB = ~8GB of data.

#### Long-running stability test
```bash
./run_benchmark.sh \
  --duration 3600 \
  --workers 100 \
  --workload mixed_50_50
```

#### SQLite backend comparison
```bash
./run_benchmark.sh --engine sqlite --duration 300
```

### Using Mix Directly

You can also run the benchmark directly with Mix:

```bash
ERL_FLAGS="+SDio 128" mix run lib/benchmark_runner.exs \
  duration=600 \
  workers=100 \
  tables=10 \
  rows=10000 \
  value_size=1024 \
  workload=mixed_50_50 \
  engine=rocksdb
```

### Using from IEx

```elixir
iex -S mix

# Run with defaults
CozodbBenchmark.run()

# Run with custom configuration
CozodbBenchmark.run(
  duration_seconds: 300,
  num_workers: 50,
  num_tables: 10,
  seed_rows: 10_000,
  value_size: 2048,
  workload: :mixed_80_20_wr,
  engine: :rocksdb
)
```

## Output

### Console Output

The benchmark prints progress and summary information:

```
============================================================
CozoDB Sustained Load Benchmark
============================================================

VM Configuration:
  Dirty IO Schedulers: 128

Benchmark Configuration:
  Duration:    600s
  Workers:     100
  ...

Results Summary:
  Total Operations: 21,456,789
  Total Reads: 10,728,394
  Total Writes: 10,728,395
  Total Errors: 1,234

Errors by Type:
  resource_busy: 1,234

Report generated: ./benchmark_reports/benchmark_report_20260116T120000.html
```

### HTML Report

Each benchmark run generates an interactive HTML report with:

- **Configuration summary** - All benchmark parameters
- **Results summary** - Total operations, throughput, error count
- **Latency statistics** - Average, P50, P90, P95, P99
- **Errors by type** - Breakdown of error categories
- **Time-series charts**:
  - Throughput over time (ops/sec)
  - Errors over time
  - Memory usage over time (MB)
  - Latency over time (avg and P99)

Open the report in your browser:
```bash
open ./benchmark_reports/benchmark_report_*.html
```

### JSON Data

Raw metrics are also exported as JSON for further analysis:

```bash
cat ./benchmark_reports/benchmark_data_*.json | jq .
```

The JSON includes:
- Full configuration
- Summary statistics
- Time-series data with per-second metrics

## Benchmark Phases

The benchmark runs in three phases to help identify memory leaks and stability issues:

1. **Load Phase** - Full concurrency with all workers active for the specified duration
2. **Ramp-down Phase** - Workers are gradually stopped in 10 steps over the rampdown duration
3. **Cooldown Phase** - 10 seconds with zero workers to observe memory behavior

This phased approach helps detect memory leaks: if memory usage doesn't decrease during ramp-down and cooldown phases, there may be a leak.

## Understanding the Results

### Active Workers

Shows the number of active workers over time. You'll see:
- Constant line during load phase
- Stepped decrease during ramp-down
- Zero during cooldown

### CPU Usage

Scheduler utilization percentage. This shows how much of the Erlang VM's processing capacity is being used.

### Throughput

Operations per second. Higher is better. Watch for:
- Declining throughput over time (indicates degradation)
- High variance (indicates contention issues)

### Errors

The benchmark tracks these error types:
- `resource_busy` - RocksDB contention (common under high write load)
- `database_locked` - SQLite locking (expected with SQLite)
- `timeout` - Operation timeouts
- `try_again` - Transient errors

Some errors under heavy load are expected. Watch for:
- Error rate increasing over time
- Error rate > 1% of operations

### Memory

The benchmark tracks two types of memory:

- **OS Process RSS** (red line) - Total process memory from the operating system's perspective. This includes:
  - BEAM/Erlang managed memory
  - NIF allocations (like RocksDB's internal memory, block cache, memtables)
  - Any other memory allocated outside the BEAM

- **BEAM Memory** (blue line) - Memory managed by the Erlang VM only

**Why track both?** CozoDB uses RocksDB which allocates memory via NIFs that the BEAM cannot see. If you only look at BEAM memory, you'll miss significant memory usage from RocksDB's block cache, write buffers, and other internal structures.

Watch for:
- Continuous growth in either metric (indicates leak)
- OS memory much higher than BEAM memory (normal - shows RocksDB memory)
- Memory not decreasing during ramp-down/cooldown phases (indicates leak)

### Latency

Response time in milliseconds. Watch for:
- P99 latency spikes
- Latency increasing over time
- Large gap between avg and P99 (indicates tail latency issues)

## Tuning Recommendations

### Dirty IO Schedulers

The `+SDio` parameter controls how many concurrent NIF operations CozoDB can handle. Recommendations:

- **Default (128)**: Good for most workloads
- **Lower (32-64)**: If you see high memory usage or want to limit concurrency
- **Higher (256+)**: If you have many CPU cores and high concurrency requirements

### Workers vs Schedulers

As a rule of thumb:
- `workers` ≤ `dirty-io-schedulers` for optimal performance
- `workers` > `dirty-io-schedulers` will cause queuing

### RocksDB vs SQLite

- **RocksDB**: Better for write-heavy and concurrent workloads
- **SQLite**: Simpler, but limited concurrent write support
- **Memory**: Fastest, but no persistence (good for baseline comparison)

## Troubleshooting

### High Error Rate

If you see many `resource_busy` errors:
1. Reduce `--workers`
2. Increase `--dirty-io-schedulers`
3. Use fewer tables to reduce contention

### Out of Memory

If the benchmark crashes with OOM:
1. Reduce `--rows` and `--value-size`
2. Reduce `--workers`
3. Monitor memory chart and stop before exhaustion

### Slow Seeding

If table seeding takes too long:
1. Reduce `--rows`
2. Reduce `--value-size`
3. Use `--engine mem` for faster seeding (no persistence)

## License

Apache License 2.0 - See the main CozoDB project for details.
