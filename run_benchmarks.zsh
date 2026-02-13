#!/usr/bin/env zsh
set -uo pipefail

# ─── Configuration ───────────────────────────────────────────────────────────
BENCHMARK_DIR="/home/eike/dev/studium/uuid-benchmark"
RESULTS_DIR="${BENCHMARK_DIR}/results"
LOG_DIR="${BENCHMARK_DIR}/logs"
BINARY="${BENCHMARK_DIR}/benchmark"
NUM_RUNS=5
BATCH_SIZE=100

# ─── Setup ───────────────────────────────────────────────────────────────────
cd "$BENCHMARK_DIR"
mkdir -p "$RESULTS_DIR" "$LOG_DIR"

echo "Building benchmark binary..."
go build -o "$BINARY" ./cmd/benchmark || { echo "Build failed"; exit 1; }
echo "Build OK"

FAILED=()
SKIPPED=0
COMPLETED=0
START_TIME=$SECONDS

run_benchmark() {
    local db=$1 scenario=$2 records=$3 ops=$4 conns=$5 output=$6
    local logfile="${LOG_DIR}/$(basename "$output" .csv).log"

    if [[ -f "$output" ]]; then
        echo "  SKIP  $output (exists)"
        ((SKIPPED++))
        return 0
    fi

    local label="${db} | ${scenario} | ${records} records | ${ops} ops | ${conns} conn"
    echo -n "  RUN   ${label} ..."

    local t0=$SECONDS
    "$BINARY" \
        -database "$db" \
        -scenario "$scenario" \
        -num-records "$records" \
        -num-ops "$ops" \
        -batch-size "$BATCH_SIZE" \
        -connections "$conns" \
        -num-runs "$NUM_RUNS" \
        -output "$output" \
        > "$logfile" 2>&1

    local rc=$?
    local elapsed=$(( SECONDS - t0 ))

    if [[ $rc -eq 0 ]]; then
        echo " OK (${elapsed}s)"
        ((COMPLETED++))
    else
        echo " FAILED (${elapsed}s) — see $logfile"
        FAILED+=("$label")
    fi
    return $rc
}

DATABASES=(postgres mysql mongodb cassandra)
SCALE_SCENARIOS=(insert-performance read-performance update-performance)

# ═══════════════════════════════════════════════════════════════════════════════
#  Tier 1a: 100K records — insert, read, update — 1 connection (fast, ~3h)
#
#  Establishes the small-dataset baseline where UUID ordering effects are
#  expected to be minimal. Provides the first data point for scaling charts.
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  TIER 1a: 100K records — insert + read + update — 1 connection"
echo "════════════════════════════════════════════════════════════════"
for db in "${DATABASES[@]}"; do
    for scenario in "${SCALE_SCENARIOS[@]}"; do
        run_benchmark "$db" "$scenario" 100000 100000 1 \
            "${RESULTS_DIR}/${db}_100k_${scenario}_1conn.csv" || true
    done
done

# ═══════════════════════════════════════════════════════════════════════════════
#  Tier 1b: 1M records — all 5 scenarios — 1 connection (core results, ~20h)
#
#  Primary dataset for the evaluation chapter. Tests H1, H2, H4.
#  All 5 scenarios × 6 key types × 4 databases × 5 runs = 600 container runs.
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  TIER 1b: 1M records — all scenarios — 1 connection"
echo "════════════════════════════════════════════════════════════════"
for db in "${DATABASES[@]}"; do
    run_benchmark "$db" all 1000000 1000000 1 \
        "${RESULTS_DIR}/${db}_1m_all_1conn.csv" || true
done

# ═══════════════════════════════════════════════════════════════════════════════
#  Tier 1c: 1M records — all 5 scenarios — 8 connections (~20h)
#
#  Tests lock contention / last-page problem (Penar 2020). Sequential keys
#  may lose throughput advantage under concurrency. Structural metrics
#  (page splits, sizes) should be unaffected by connection count.
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  TIER 1c: 1M records — all scenarios — 8 connections"
echo "════════════════════════════════════════════════════════════════"
for db in "${DATABASES[@]}"; do
    run_benchmark "$db" all 1000000 1000000 8 \
        "${RESULTS_DIR}/${db}_1m_all_8conn.csv" || true
done

# ═══════════════════════════════════════════════════════════════════════════════
#  Tier 2: 10M records — insert, read, update — 1 connection (slow, ~30h+)
#
#  Tests H3 (cache effects at scale). At 10M records the dataset approaches
#  or exceeds the ~4 GB buffer pool, making cache hit ratio differences
#  between key types observable. Also shows whether page split and
#  fragmentation effects compound at larger scale.
#  Read/update use 10M ops (same 1:1 ratio as other tiers) for consistent
#  cache warmup behavior across scale points.
# ═══════════════════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  TIER 2: 10M records — insert + read + update — 1 connection"
echo "════════════════════════════════════════════════════════════════"
for db in "${DATABASES[@]}"; do
    run_benchmark "$db" insert-performance 10000000 10000000 1 \
        "${RESULTS_DIR}/${db}_10m_insert-performance_1conn.csv" || true
    run_benchmark "$db" read-performance 10000000 10000000 1 \
        "${RESULTS_DIR}/${db}_10m_read-performance_1conn.csv" || true
    run_benchmark "$db" update-performance 10000000 10000000 1 \
        "${RESULTS_DIR}/${db}_10m_update-performance_1conn.csv" || true
done

# ─── Summary ─────────────────────────────────────────────────────────────────
TOTAL_TIME=$(( SECONDS - START_TIME ))
HOURS=$(( TOTAL_TIME / 3600 ))
MINS=$(( (TOTAL_TIME % 3600) / 60 ))

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "  DONE — ${HOURS}h ${MINS}m elapsed"
echo "  Completed: ${COMPLETED}  Skipped: ${SKIPPED}  Failed: ${#FAILED[@]}"
echo "════════════════════════════════════════════════════════════════"

if [[ ${#FAILED[@]} -gt 0 ]]; then
    echo ""
    echo "Failed runs:"
    for f in "${FAILED[@]}"; do
        echo "  - $f"
    done
    echo ""
    echo "Check logs in ${LOG_DIR}/"
fi

echo ""
echo "Results in ${RESULTS_DIR}/"
echo ""
echo "Run matrix summary:"
echo "  Tier 1a: 4 DBs × 3 scenarios × 100K × 1 conn × ${NUM_RUNS} runs"
echo "  Tier 1b: 4 DBs × 5 scenarios × 1M   × 1 conn × ${NUM_RUNS} runs"
echo "  Tier 1c: 4 DBs × 5 scenarios × 1M   × 8 conn × ${NUM_RUNS} runs"
echo "  Tier 2:  4 DBs × 3 scenarios × 10M   × 1 conn × ${NUM_RUNS} runs"
