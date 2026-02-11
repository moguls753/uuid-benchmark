#!/bin/bash
set -euo pipefail

# Comprehensive YCSB vs uuid-benchmark comparison
# Usage: ./run-comparison.sh [insert|read] [num_records]
# Examples:
#   ./run-comparison.sh insert          # 10k records (default)
#   ./run-comparison.sh insert 100000   # 100k records
#   ./run-comparison.sh read 1000000    # 1M records

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BENCHMARK_BIN="$PROJECT_ROOT/uuid-benchmark"
RESULTS_DIR="$SCRIPT_DIR/results"

# Check dependencies
if [ ! -f "$BENCHMARK_BIN" ]; then
    echo "Error: uuid-benchmark binary not found at $BENCHMARK_BIN"
    echo "Build it first: cd $PROJECT_ROOT && go build -o uuid-benchmark cmd/benchmark/main.go"
    exit 1
fi

SCENARIO=${1:-insert}
NUM_RECORDS=${2:-10000}

# Scenario mappings - aligned for exact comparison
case $SCENARIO in
    insert)
        UUID_SCENARIO="insert-performance"
        UUID_FLAGS="-num-records=$NUM_RECORDS -connections=10 -batch-size=1"
        YCSB_WORKLOAD="insert"
        DESC="100% insert ($NUM_RECORDS records, 10 connections, batch-size=1)"
        ;;
    read)
        UUID_SCENARIO="read-performance"
        UUID_FLAGS="-num-records=$NUM_RECORDS -num-ops=$NUM_RECORDS -connections=10"
        YCSB_WORKLOAD="read"
        DESC="100% read ($NUM_RECORDS records, $NUM_RECORDS ops, 10 connections)"
        ;;
    *)
        echo "Error: Unknown scenario: $SCENARIO"
        echo "Available: insert, read"
        echo ""
        echo "Usage: $0 [insert|read] [num_records]"
        echo "Examples:"
        echo "  $0 insert 10000      # 10k inserts"
        echo "  $0 read 100000       # 100k reads"
        exit 1
        ;;
esac

echo "=========================================="
echo "YCSB vs uuid-benchmark Validation"
echo "=========================================="
echo "Scenario: $SCENARIO"
echo "Records: $NUM_RECORDS"
echo "Description: $DESC"
echo ""
echo "Architecture: Both tools run inside containers"
echo "  - YCSB: go-ycsb inside container → localhost"
echo "  - uuid-benchmark: pgbench inside container → localhost"
echo "=========================================="
echo ""

mkdir -p "$RESULTS_DIR"

echo "Step 1/2: Running YCSB..."
echo ""
bash "$SCRIPT_DIR/run-ycsb.sh" "$YCSB_WORKLOAD" "$NUM_RECORDS"

echo ""
echo "Step 2/2: Running uuid-benchmark..."
echo ""

UUID_OUTPUT="$RESULTS_DIR/uuid_benchmark_${SCENARIO}_${NUM_RECORDS}_$(date +%Y%m%d_%H%M%S).txt"
cd "$PROJECT_ROOT"
$BENCHMARK_BIN -scenario="$UUID_SCENARIO" $UUID_FLAGS | tee "$UUID_OUTPUT"
cd "$SCRIPT_DIR"

echo ""
echo "=========================================="
echo "Comparison Complete - Displaying Results"
echo "=========================================="
echo ""

# Automatically display comparison
bash "$SCRIPT_DIR/compare-results.sh"

echo ""
echo "Results saved in: $RESULTS_DIR"
echo "  YCSB: $(ls -t "$RESULTS_DIR"/ycsb_*.txt 2>/dev/null | head -1)"
echo "  UUID: $UUID_OUTPUT"
echo ""
