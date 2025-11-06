#!/bin/bash

set -e

# Usage check
if [ $# -lt 2 ]; then
    echo "Usage: $0 <database> <key-type> [num-records]"
    echo ""
    echo "Examples:"
    echo "  $0 postgres bigserial 10000"
    echo "  $0 postgres uuidv4 50000"
    echo ""
    echo "Databases: postgres, mysql, mongo, cassandra"
    echo "Key types: bigserial, uuidv4, uuidv7, uuidv1, ulid"
    exit 1
fi

DATABASE=$1
KEY_TYPE=$2
NUM_RECORDS=${3:-10000}

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "========================================"
echo "Database:   $DATABASE"
echo "Key Type:   $KEY_TYPE"
echo "Records:    $NUM_RECORDS"
echo "========================================"
echo ""

# Build if needed
if [ ! -f "$PROJECT_ROOT/uuid-benchmark" ]; then
    echo "Building benchmark binary..."
    go build -o uuid-benchmark ./cmd/benchmark
    echo ""
fi

# Start database
echo "Starting $DATABASE container..."
docker compose -f "docker/docker-compose.$DATABASE.yml" up -d
echo ""

# Wait for database to be ready
echo "Waiting for $DATABASE to be ready..."

CONTAINER_NAME="uuid-bench-$DATABASE"
if [ "$DATABASE" == "mongo" ]; then
    CONTAINER_NAME="uuid-bench-mongodb"
fi

MAX_ATTEMPTS=60
ATTEMPT=0

while [ $ATTEMPT -lt $MAX_ATTEMPTS ]; do
    case "$DATABASE" in
        postgres)
            if docker exec "$CONTAINER_NAME" pg_isready -U benchmark >/dev/null 2>&1; then
                echo "✓ $DATABASE is ready!"
                break
            fi
            ;;
        mysql)
            if docker exec "$CONTAINER_NAME" mysql --user=benchmark --password=benchmark123 --execute="SELECT 1" >/dev/null 2>&1; then
                echo "✓ $DATABASE is ready!"
                break
            fi
            ;;
        mongo)
            if docker exec "$CONTAINER_NAME" mongosh --quiet --eval "db.adminCommand('ping').ok" >/dev/null 2>&1; then
                echo "✓ $DATABASE is ready!"
                break
            fi
            ;;
        cassandra)
            if docker exec "$CONTAINER_NAME" cqlsh -e "describe cluster" >/dev/null 2>&1; then
                echo "✓ $DATABASE is ready!"
                break
            fi
            ;;
        *)
            echo "Unknown database: $DATABASE"
            exit 1
            ;;
    esac

    ATTEMPT=$((ATTEMPT + 1))
    echo -n "."
    sleep 2
done

if [ $ATTEMPT -eq $MAX_ATTEMPTS ]; then
    echo ""
    echo "Error: $DATABASE failed to start after $((MAX_ATTEMPTS * 2)) seconds"
    exit 1
fi

echo ""

# Run benchmark
echo "Running benchmark..."
echo ""
./uuid-benchmark --key-type "$KEY_TYPE" --num-records "$NUM_RECORDS"

# Cleanup
echo ""
echo "Cleaning up..."
docker compose -f "docker/docker-compose.$DATABASE.yml" down -v >/dev/null 2>&1

echo ""
echo "✓ Test completed!"
