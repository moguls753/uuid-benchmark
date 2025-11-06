#!/bin/bash

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DOCKER_DIR="$PROJECT_ROOT/docker"

# Databases to benchmark
DATABASES=("postgres" "mysql" "mongo" "cassandra")

# Function to print colored messages
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to start a database
start_database() {
    local db=$1
    log_info "Starting $db database..."
    docker compose -f "$DOCKER_DIR/docker-compose.$db.yml" up -d
}

# Function to check if database is ready (one-time check, no continuous overhead)
wait_for_ready() {
    local db=$1
    local container_name="uuid-bench-$db"
    local max_attempts=60
    local attempt=0

    if [ "$db" == "mongo" ]; then
        container_name="uuid-bench-mongodb"
    fi

    log_info "Waiting for $db to accept connections..."

    while [ $attempt -lt $max_attempts ]; do
        case "$db" in
            "postgres")
                if docker exec "$container_name" pg_isready -U benchmark >/dev/null 2>&1; then
                    log_info "$db is ready!"
                    return 0
                fi
                ;;
            "mysql")
                if docker exec "$container_name" mysql --user=benchmark --password=benchmark123 --execute="SELECT 1" >/dev/null 2>&1; then
                    log_info "$db is ready!"
                    return 0
                fi
                ;;
            "mongo")
                if docker exec "$container_name" mongosh --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; then
                    log_info "$db is ready!"
                    return 0
                fi
                ;;
            "cassandra")
                if docker exec "$container_name" cqlsh -e "describe cluster" >/dev/null 2>&1; then
                    log_info "$db is ready!"
                    return 0
                fi
                ;;
        esac

        attempt=$((attempt + 1))
        echo -n "."
        sleep 2
    done

    log_error "$db failed to become ready after $((max_attempts * 2)) seconds"
    return 1
}

# Function to stop and cleanup database
cleanup_database() {
    local db=$1
    log_info "Stopping and cleaning up $db..."
    docker compose -f "$DOCKER_DIR/docker-compose.$db.yml" down -v
}

# Function to run benchmark for a database
run_benchmark() {
    local db=$1

    log_info "=========================================="
    log_info "Running benchmark for: $db"
    log_info "=========================================="

    # Start the database
    start_database "$db"

    # Wait for it to be ready
    if ! wait_for_ready "$db"; then
        log_error "Failed to start $db. Skipping..."
        cleanup_database "$db"
        return 1
    fi

    # TODO: Run actual benchmark here
    log_warn "Benchmark execution not yet implemented"
    log_warn "Add your benchmark command here, e.g.:"
    log_warn "  ./cmd/benchmark/benchmark --database $db --workload insert"

    # For now, just sleep to simulate work
    log_info "Database is ready for benchmarking (no continuous healthcheck overhead!)"
    sleep 5

    # Cleanup
    cleanup_database "$db"

    log_info "Completed benchmark for $db"
    echo ""
}

# Function to show usage
show_usage() {
    echo "Usage: $0 [DATABASE...]"
    echo ""
    echo "Run benchmarks for specified databases, or all if none specified."
    echo ""
    echo "Available databases:"
    echo "  postgres   - PostgreSQL"
    echo "  mysql      - MySQL"
    echo "  mongo      - MongoDB"
    echo "  cassandra  - Apache Cassandra"
    echo ""
    echo "Examples:"
    echo "  $0                    # Run all databases"
    echo "  $0 postgres           # Run PostgreSQL only"
    echo "  $0 postgres mysql     # Run PostgreSQL and MySQL"
    echo ""
}

# Function to validate database name
is_valid_database() {
    local db=$1
    for valid_db in "${DATABASES[@]}"; do
        if [ "$db" == "$valid_db" ]; then
            return 0
        fi
    done
    return 1
}

# Main execution
main() {
    log_info "UUID Benchmark Orchestration Script"
    log_info "Project root: $PROJECT_ROOT"
    echo ""

    # Check if docker compose is available
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed or not in PATH"
        exit 1
    fi

    # Parse arguments
    local databases_to_run=()

    if [ $# -eq 0 ]; then
        # No arguments - run all databases
        databases_to_run=("${DATABASES[@]}")
        log_info "No databases specified, running all: ${DATABASES[*]}"
    else
        # Validate and collect specified databases
        for db in "$@"; do
            if [ "$db" == "-h" ] || [ "$db" == "--help" ]; then
                show_usage
                exit 0
            fi

            if is_valid_database "$db"; then
                databases_to_run+=("$db")
            else
                log_error "Invalid database: $db"
                echo ""
                show_usage
                exit 1
            fi
        done

        log_info "Running benchmarks for: ${databases_to_run[*]}"
    fi

    echo ""

    # Run benchmarks for each database sequentially
    for db in "${databases_to_run[@]}"; do
        run_benchmark "$db" || log_warn "Benchmark for $db completed with errors"
    done

    log_info "=========================================="
    log_info "All benchmarks completed!"
    log_info "=========================================="
}

# Run main function
main "$@"
