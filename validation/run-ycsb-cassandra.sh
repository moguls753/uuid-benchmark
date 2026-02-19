#!/bin/bash
set -euo pipefail

# YCSB validation for Cassandra — compares go-ycsb vs custom Go workload binary
# Verifies that throughput/latency ratios (insert vs read vs update) are consistent
# between the industry-standard YCSB tool and our custom workload binary.
#
# Usage: ./run-ycsb-cassandra.sh [num_records] [num_ops]
#   num_records: Number of records to insert (default: 100000)
#   num_ops:     Number of operations for read/update (default: 10000)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

YCSB_BIN="${YCSB_BIN:-$(cd "$PROJECT_ROOT/.." && pwd)/go-ycsb/bin/go-ycsb}"
RESULTS_DIR="$SCRIPT_DIR/results"
DOCKER_COMPOSE_FILE="$PROJECT_ROOT/docker/docker-compose.cassandra.yml"

NUM_RECORDS=${1:-100000}
NUM_OPS=${2:-10000}
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Check dependencies
if [ ! -f "$YCSB_BIN" ]; then
    echo "Error: go-ycsb binary not found at $YCSB_BIN"
    echo "Please build go-ycsb first:"
    echo "  cd $(dirname "$YCSB_BIN")/.."
    echo "  make"
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Error: docker not found"
    exit 1
fi

mkdir -p "$RESULTS_DIR"

YCSB_OUTPUT="$RESULTS_DIR/ycsb_cassandra_${NUM_RECORDS}_${TIMESTAMP}.txt"
WORKLOAD_OUTPUT="$RESULTS_DIR/workload_cassandra_${NUM_RECORDS}_${TIMESTAMP}.txt"

cleanup() {
    echo ""
    echo "Cleaning up Cassandra container..."
    docker compose -f "$DOCKER_COMPOSE_FILE" down --volumes --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT INT TERM

echo "=========================================="
echo "Cassandra YCSB Validation"
echo "=========================================="
echo "Records:    $NUM_RECORDS"
echo "Ops:        $NUM_OPS"
echo "Threads:    1 (single-threaded for fair comparison)"
echo ""
echo "Compares: go-ycsb (industry standard) vs Go workload binary (custom)"
echo "Both run inside the container → localhost, no network overhead"
echo "=========================================="
echo ""

# ─── Start Cassandra ─────────────────────────────────────────────────────────
echo "Starting Cassandra container..."
docker compose -f "$DOCKER_COMPOSE_FILE" up -d

echo "Waiting for Cassandra to initialize..."
for i in {1..60}; do
    if docker exec uuid-bench-cassandra cqlsh -e "SELECT now() FROM system.local" > /dev/null 2>&1; then
        echo "Cassandra ready"
        break
    fi
    if [ $i -eq 60 ]; then
        echo "Error: Cassandra failed to start within 60 seconds"
        exit 1
    fi
    sleep 2
done

# ─── Create YCSB keyspace and table ──────────────────────────────────────────
echo "Creating YCSB keyspace and table..."
docker exec uuid-bench-cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS ycsb
  WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS ycsb.usertable (
    y_id varchar PRIMARY KEY,
    field0 varchar, field1 varchar, field2 varchar, field3 varchar,
    field4 varchar, field5 varchar, field6 varchar, field7 varchar,
    field8 varchar, field9 varchar
);"

# ─── Create workload binary keyspace and table ───────────────────────────────
echo "Creating workload binary keyspace and table..."
docker exec uuid-bench-cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS uuid_benchmark
  WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':1};
CREATE TABLE IF NOT EXISTS uuid_benchmark.bench (
    bucket int,
    id bigint,
    payload blob,
    PRIMARY KEY ((bucket), id)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};"

# ─── Copy binaries into container ─────────────────────────────────────────────
echo "Copying go-ycsb binary into container..."
docker cp "$YCSB_BIN" uuid-bench-cassandra:/tmp/go-ycsb
docker exec uuid-bench-cassandra chmod +x /tmp/go-ycsb

echo "Building workload binary..."
WORKLOAD_TMP=$(mktemp -d)
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o "$WORKLOAD_TMP/workload" "$PROJECT_ROOT/cmd/workload/main.go"
docker cp "$WORKLOAD_TMP/workload" uuid-bench-cassandra:/tmp/workload
docker exec uuid-bench-cassandra chmod +x /tmp/workload
rm -rf "$WORKLOAD_TMP"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 1: INSERT — go-ycsb
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 1: go-ycsb INSERT ($NUM_RECORDS records)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/go-ycsb load cassandra \
    -p cassandra.cluster=127.0.0.1:9042 \
    -p cassandra.keyspace=ycsb \
    -p recordcount="$NUM_RECORDS" \
    -p operationcount="$NUM_RECORDS" \
    -p threadcount=1 \
    -p fieldcount=10 \
    -p fieldlength=100 \
    2>&1 | tee "$YCSB_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 2: READ — go-ycsb
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 2: go-ycsb READ ($NUM_OPS ops)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/go-ycsb run cassandra \
    -p cassandra.cluster=127.0.0.1:9042 \
    -p cassandra.keyspace=ycsb \
    -p recordcount="$NUM_RECORDS" \
    -p operationcount="$NUM_OPS" \
    -p threadcount=1 \
    -p workload=core \
    -p readproportion=1.0 \
    -p updateproportion=0.0 \
    -p scanproportion=0.0 \
    -p insertproportion=0.0 \
    -p requestdistribution=uniform \
    2>&1 | tee -a "$YCSB_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 3: UPDATE — go-ycsb
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 3: go-ycsb UPDATE ($NUM_OPS ops)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/go-ycsb run cassandra \
    -p cassandra.cluster=127.0.0.1:9042 \
    -p cassandra.keyspace=ycsb \
    -p recordcount="$NUM_RECORDS" \
    -p operationcount="$NUM_OPS" \
    -p threadcount=1 \
    -p workload=core \
    -p readproportion=0.0 \
    -p updateproportion=1.0 \
    -p scanproportion=0.0 \
    -p insertproportion=0.0 \
    -p requestdistribution=uniform \
    2>&1 | tee -a "$YCSB_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Reset: drop and recreate workload table for clean insert
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "Resetting workload binary table..."
docker exec uuid-bench-cassandra cqlsh -e "
DROP TABLE IF EXISTS uuid_benchmark.bench;
CREATE TABLE uuid_benchmark.bench (
    bucket int,
    id bigint,
    payload blob,
    PRIMARY KEY ((bucket), id)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 4: INSERT — workload binary
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 4: Workload binary INSERT ($NUM_RECORDS records)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/workload \
    --db-type=cassandra \
    --op=insert \
    --key-type=sequential \
    --num-records="$NUM_RECORDS" \
    --batch-size=1 \
    --threads=1 \
    --connection-string=127.0.0.1 \
    2>&1 | tee "$WORKLOAD_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 5: READ — workload binary
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 5: Workload binary READ ($NUM_OPS ops)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/workload \
    --db-type=cassandra \
    --op=read \
    --key-type=sequential \
    --num-ops="$NUM_OPS" \
    --threads=1 \
    --connection-string=127.0.0.1 \
    2>&1 | tee -a "$WORKLOAD_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Phase 6: UPDATE — workload binary
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Phase 6: Workload binary UPDATE ($NUM_OPS ops)"
echo "=========================================="
docker exec uuid-bench-cassandra /tmp/workload \
    --db-type=cassandra \
    --op=update \
    --key-type=sequential \
    --num-ops="$NUM_OPS" \
    --threads=1 \
    --connection-string=127.0.0.1 \
    2>&1 | tee -a "$WORKLOAD_OUTPUT"

# ═══════════════════════════════════════════════════════════════════════════════
#  Summary
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "=========================================="
echo "Cassandra YCSB Validation Complete"
echo "=========================================="
echo ""
echo "Results saved:"
echo "  go-ycsb:          $YCSB_OUTPUT"
echo "  Workload binary:  $WORKLOAD_OUTPUT"
echo ""
echo "Compare throughput ratios (insert:read:update) between both tools."
echo "If ratios match, the workload binary methodology is validated."
echo ""
