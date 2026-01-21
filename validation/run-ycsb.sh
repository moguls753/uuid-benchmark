#!/bin/bash
set -euo pipefail

# Script to run go-ycsb inside PostgreSQL container for architectural parity with uuid-benchmark
# Usage: ./run-ycsb.sh <workload_name>
#   workload_name: read_heavy, balanced, insert_heavy, mixed_read_heavy

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Configurable paths
YCSB_BIN="${YCSB_BIN:-$(cd "$PROJECT_ROOT/.." && pwd)/go-ycsb/bin/go-ycsb}"
WORKLOAD_DIR="$SCRIPT_DIR/workloads"
RESULTS_DIR="$SCRIPT_DIR/results"

# PostgreSQL configuration
PG_IMAGE="postgres:18"
PG_DB="uuid_benchmark"
PG_USER="benchmark"
PG_PASSWORD="benchmark123"
TABLE_NAME="ycsb_bigserial"

# Validation
if [ $# -lt 1 ]; then
    echo "Usage: $0 <workload_name> [num_records]"
    echo "  workload_name: insert, read"
    echo "  num_records: Number of records (default: 10000)"
    exit 1
fi

WORKLOAD_NAME=$1
NUM_RECORDS=${2:-10000}
WORKLOAD_FILE="$WORKLOAD_DIR/workload_$WORKLOAD_NAME"

# Check dependencies
for cmd in docker; do
    if ! command -v "$cmd" &> /dev/null; then
        echo "Error: Required command '$cmd' not found"
        exit 1
    fi
done

if [ ! -f "$YCSB_BIN" ]; then
    echo "Error: go-ycsb binary not found at $YCSB_BIN"
    echo "Please build go-ycsb first:"
    echo "  cd $(dirname "$YCSB_BIN")/.."
    echo "  make"
    exit 1
fi

if [ ! -f "$WORKLOAD_FILE" ]; then
    echo "Error: Workload file not found: $WORKLOAD_FILE"
    exit 1
fi

mkdir -p "$RESULTS_DIR"
OUTPUT_FILE="$RESULTS_DIR/ycsb_${WORKLOAD_NAME}_${NUM_RECORDS}_$(date +%Y%m%d_%H%M%S).txt"

echo "=========================================="
echo "Running YCSB Validation (In-Container)"
echo "=========================================="
echo "Workload: $WORKLOAD_NAME"
echo "Table: $TABLE_NAME"
echo "Architecture: go-ycsb inside container → localhost"
echo "=========================================="
echo ""

# Start fresh PostgreSQL container
echo "Starting PostgreSQL container..."
CONTAINER_ID=$(docker run -d \
    --rm \
    -e POSTGRES_DB="$PG_DB" \
    -e POSTGRES_USER="$PG_USER" \
    -e POSTGRES_PASSWORD="$PG_PASSWORD" \
    "$PG_IMAGE" \
    postgres \
    -c shared_preload_libraries='pg_stat_statements' \
    -c max_connections=200)

echo "Container ID: $CONTAINER_ID"

# Cleanup on exit
cleanup() {
    if [ -n "${CONTAINER_ID:-}" ]; then
        echo ""
        echo "Cleaning up container..."
        docker stop "$CONTAINER_ID" > /dev/null 2>&1 || true
    fi
}
trap cleanup EXIT INT TERM

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to start..."
for i in {1..30}; do
    if docker exec "$CONTAINER_ID" pg_isready -U "$PG_USER" > /dev/null 2>&1; then
        echo "PostgreSQL ready"
        break
    fi
    if [ $i -eq 30 ]; then
        echo "Error: PostgreSQL failed to start within 30 seconds"
        exit 1
    fi
    sleep 1
done

# Copy go-ycsb binary into container
echo "Copying go-ycsb binary into container..."
docker cp "$YCSB_BIN" "$CONTAINER_ID:/tmp/go-ycsb"
docker exec "$CONTAINER_ID" chmod +x /tmp/go-ycsb

# Copy workload file
echo "Copying workload configuration..."
docker cp "$WORKLOAD_FILE" "$CONTAINER_ID:/tmp/workload"

# Create table (BIGSERIAL only - YCSB uses numeric keys)
echo "Creating table..."
docker exec "$CONTAINER_ID" psql -U "$PG_USER" -d "$PG_DB" <<EOF
CREATE TABLE $TABLE_NAME (
    ycsb_key BIGSERIAL PRIMARY KEY,
    field0 TEXT,
    field1 TEXT,
    field2 TEXT,
    field3 TEXT,
    field4 TEXT,
    field5 TEXT,
    field6 TEXT,
    field7 TEXT,
    field8 TEXT,
    field9 TEXT
);
EOF

echo "Table created successfully"
echo ""

# Run YCSB load phase inside container
echo "Running YCSB load phase..."
docker exec "$CONTAINER_ID" /tmp/go-ycsb load postgresql \
    -P /tmp/workload \
    -p pg.host=localhost \
    -p pg.port=5432 \
    -p pg.user="$PG_USER" \
    -p pg.password="$PG_PASSWORD" \
    -p pg.db="$PG_DB" \
    -p pg.tablename="$TABLE_NAME" \
    -p threadcount=10 \
    -p recordcount="$NUM_RECORDS" \
    -p operationcount="$NUM_RECORDS" \
    | tee -a "$OUTPUT_FILE"

echo ""
echo "Running YCSB run phase..."
docker exec "$CONTAINER_ID" /tmp/go-ycsb run postgresql \
    -P /tmp/workload \
    -p pg.host=localhost \
    -p pg.port=5432 \
    -p pg.user="$PG_USER" \
    -p pg.password="$PG_PASSWORD" \
    -p pg.db="$PG_DB" \
    -p pg.tablename="$TABLE_NAME" \
    -p threadcount=10 \
    -p recordcount="$NUM_RECORDS" \
    -p operationcount="$NUM_RECORDS" \
    | tee -a "$OUTPUT_FILE"

echo ""
echo "=========================================="
echo "YCSB run completed"
echo "Results saved to: $OUTPUT_FILE"
echo "Container will be stopped and removed"
echo "=========================================="
