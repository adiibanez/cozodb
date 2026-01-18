#!/bin/bash
#
# CozoDB Sustained Load Benchmark Runner
#
# This script runs the benchmark with configurable Erlang VM and jemalloc parameters.
#
# Usage:
#   ./run_benchmark.sh [OPTIONS]
#
# Benchmark Options:
#   --duration N               Duration in seconds (default: 600)
#   --rampdown N               Ramp-down duration in seconds (default: 60)
#   --workers N                Number of concurrent workers (default: 100)
#   --tables N                 Number of tables (default: 10)
#   --rows N                   Rows per table (default: 10000)
#   --value-size N             Size of value field in bytes (default: 1024)
#   --workload TYPE            Workload type: read_only, write_only, mixed_50_50, mixed_80_20_wr (default: mixed_50_50)
#   --engine ENGINE            Database engine: rocksdb, sqlite, mem (default: rocksdb)
#   --db-path PATH             Database path (default: /tmp/cozodb_benchmark_sustained)
#   --report-dir PATH          Report output directory (default: ./benchmark_reports)
#
# Erlang VM Options:
#   --dirty-io-schedulers N    Number of dirty IO schedulers (default: 128)
#
# jemalloc Options:
#   --jemalloc-dirty-decay N   Dirty page decay time in ms (default: 1000)
#   --jemalloc-muzzy-decay N   Muzzy page decay time in ms (default: 1000)
#   --jemalloc-bg-thread BOOL  Enable background thread (default: true)
#   --jemalloc-narenas N       Number of arenas (default: auto)
#
# Other:
#   --help                     Show this help message
#
# Examples:
#   # Run with defaults (10 min, 100 workers, 128 dirty IO schedulers)
#   ./run_benchmark.sh
#
#   # Run a quick 2-minute test
#   ./run_benchmark.sh --duration 120 --rampdown 30
#
#   # Run with aggressive memory return (decay=0)
#   ./run_benchmark.sh --jemalloc-dirty-decay 0 --jemalloc-muzzy-decay 0
#
#   # Run with limited arenas for many schedulers
#   ./run_benchmark.sh --jemalloc-narenas 8
#
#   # Run a heavy write workload
#   ./run_benchmark.sh --duration 1800 --workload mixed_80_20_wr --workers 200
#

set -e

# Default values - Benchmark
DURATION=600
RAMPDOWN=60
WORKERS=100
TABLES=10
ROWS=10000
VALUE_SIZE=1024
WORKLOAD=mixed_50_50
ENGINE=rocksdb
DB_PATH="/tmp/cozodb_benchmark_sustained"
REPORT_DIR="./benchmark_reports"

# Default values - Erlang VM
DIRTY_IO_SCHEDULERS=128

# Default values - jemalloc (matching NIF defaults)
JEMALLOC_DIRTY_DECAY=1000
JEMALLOC_MUZZY_DECAY=1000
JEMALLOC_BG_THREAD=true
JEMALLOC_NARENAS=""

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        # Benchmark options
        --duration)
            DURATION="$2"
            shift 2
            ;;
        --rampdown)
            RAMPDOWN="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --tables)
            TABLES="$2"
            shift 2
            ;;
        --rows)
            ROWS="$2"
            shift 2
            ;;
        --value-size)
            VALUE_SIZE="$2"
            shift 2
            ;;
        --workload)
            WORKLOAD="$2"
            shift 2
            ;;
        --engine)
            ENGINE="$2"
            shift 2
            ;;
        --db-path)
            DB_PATH="$2"
            shift 2
            ;;
        --report-dir)
            REPORT_DIR="$2"
            shift 2
            ;;
        # Erlang VM options
        --dirty-io-schedulers)
            DIRTY_IO_SCHEDULERS="$2"
            shift 2
            ;;
        # jemalloc options
        --jemalloc-dirty-decay)
            JEMALLOC_DIRTY_DECAY="$2"
            shift 2
            ;;
        --jemalloc-muzzy-decay)
            JEMALLOC_MUZZY_DECAY="$2"
            shift 2
            ;;
        --jemalloc-bg-thread)
            JEMALLOC_BG_THREAD="$2"
            shift 2
            ;;
        --jemalloc-narenas)
            JEMALLOC_NARENAS="$2"
            shift 2
            ;;
        --help)
            head -55 "$0" | tail -50
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Change to the script's directory
cd "$(dirname "$0")"

echo "============================================================"
echo "CozoDB Sustained Load Benchmark"
echo "============================================================"
echo ""
echo "Benchmark Configuration:"
echo "  Duration:    ${DURATION}s"
echo "  Ramp-down:   ${RAMPDOWN}s"
echo "  Workers:     $WORKERS"
echo "  Tables:      $TABLES"
echo "  Rows/Table:  $ROWS"
echo "  Value Size:  ${VALUE_SIZE} bytes"
echo "  Workload:    $WORKLOAD"
echo "  Engine:      $ENGINE"
echo "  DB Path:     $DB_PATH"
echo "  Report Dir:  $REPORT_DIR"
echo ""
echo "Erlang VM Configuration:"
echo "  Dirty IO Schedulers: $DIRTY_IO_SCHEDULERS"
echo ""
echo "jemalloc Configuration:"
echo "  Dirty Decay:      ${JEMALLOC_DIRTY_DECAY}ms"
echo "  Muzzy Decay:      ${JEMALLOC_MUZZY_DECAY}ms"
echo "  Background Thread: $JEMALLOC_BG_THREAD"
echo "  Arenas:           ${JEMALLOC_NARENAS:-auto}"
echo ""
echo "============================================================"
echo ""

# Build benchmark args
BENCHMARK_ARGS="duration=$DURATION rampdown=$RAMPDOWN workers=$WORKERS tables=$TABLES rows=$ROWS value_size=$VALUE_SIZE workload=$WORKLOAD engine=$ENGINE db_path=$DB_PATH report_dir=$REPORT_DIR"

# Set Erlang VM flags
export ERL_FLAGS="+SDio $DIRTY_IO_SCHEDULERS"

# Set jemalloc configuration via environment variables
export COZODB_JEMALLOC_DIRTY_DECAY_MS="$JEMALLOC_DIRTY_DECAY"
export COZODB_JEMALLOC_MUZZY_DECAY_MS="$JEMALLOC_MUZZY_DECAY"
export COZODB_JEMALLOC_BACKGROUND_THREAD="$JEMALLOC_BG_THREAD"
if [[ -n "$JEMALLOC_NARENAS" ]]; then
    export COZODB_JEMALLOC_NARENAS="$JEMALLOC_NARENAS"
fi

exec mix run lib/benchmark_runner.exs $BENCHMARK_ARGS
