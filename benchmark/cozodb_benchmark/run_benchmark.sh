#!/bin/bash
#
# CozoDB Sustained Load Benchmark Runner
#
# This script runs the benchmark with configurable Erlang VM parameters.
#
# Usage:
#   ./run_benchmark.sh [OPTIONS]
#
# Options:
#   --dirty-io-schedulers N    Number of dirty IO schedulers (default: 128)
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
#   --help                     Show this help message
#
# Examples:
#   # Run with defaults (10 min, 100 workers, 128 dirty IO schedulers)
#   ./run_benchmark.sh
#
#   # Run a quick 1-minute test with fewer schedulers
#   ./run_benchmark.sh --duration 60 --dirty-io-schedulers 64
#
#   # Run a heavy write workload for 30 minutes
#   ./run_benchmark.sh --duration 1800 --workload mixed_80_20_wr --workers 200
#
#   # Generate large amounts of data
#   ./run_benchmark.sh --rows 100000 --value-size 8192 --duration 600
#

set -e

# Default values
DIRTY_IO_SCHEDULERS=128
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

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --dirty-io-schedulers)
            DIRTY_IO_SCHEDULERS="$2"
            shift 2
            ;;
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
        --help)
            head -40 "$0" | tail -35
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
echo "VM Configuration:"
echo "  Dirty IO Schedulers: $DIRTY_IO_SCHEDULERS"
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
echo "============================================================"
echo ""

# Build benchmark args
BENCHMARK_ARGS="duration=$DURATION rampdown=$RAMPDOWN workers=$WORKERS tables=$TABLES rows=$ROWS value_size=$VALUE_SIZE workload=$WORKLOAD engine=$ENGINE db_path=$DB_PATH report_dir=$REPORT_DIR"

# Run the benchmark with the specified dirty IO schedulers
export ERL_FLAGS="+SDio $DIRTY_IO_SCHEDULERS"

exec mix run lib/benchmark_runner.exs $BENCHMARK_ARGS
