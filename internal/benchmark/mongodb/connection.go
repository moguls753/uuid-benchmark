package mongodb

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	dbHost     = "localhost"
	dbPort     = "27018"
	dbUser     = "benchmark"
	dbPassword = "benchmark123"
	dbName     = "uuid_benchmark"

	// ContainerName is the Docker container name for MongoDB.
	ContainerName = "uuid-bench-mongodb"

	// WorkloadConnString is the connection string used by the workload binary
	// running inside the container (connects via loopback).
	WorkloadConnString = "mongodb://benchmark:benchmark123@127.0.0.1:27017/uuid_benchmark?authSource=admin"
)

func (m *MongoDBBenchmarker) Connect() error {
	connStr := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	client, err := mongo.Connect(options.Client().ApplyURI(connStr))
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	m.client = client
	m.db = client.Database(dbName)

	return nil
}

func (m *MongoDBBenchmarker) CreateCollection(keyType string) error {
	m.keyType = keyType
	m.collName = "bench"

	ctx := context.Background()

	// Drop existing collection
	if err := m.db.Collection(m.collName).Drop(ctx); err != nil {
		return fmt.Errorf("drop collection: %w", err)
	}

	// Create collection explicitly
	if err := m.db.CreateCollection(ctx, m.collName); err != nil {
		return fmt.Errorf("create collection: %w", err)
	}

	return nil
}

func (m *MongoDBBenchmarker) Close() error {
	if m.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return m.client.Disconnect(ctx)
	}
	return nil
}

func WaitForReady() error {
	connStr := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s?authSource=admin",
		dbUser, dbPassword, dbHost, dbPort, dbName)

	timeout := 60 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		client, err := mongo.Connect(options.Client().ApplyURI(connStr))
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			pingErr := client.Ping(ctx, nil)
			cancel()
			if pingErr == nil {
				client.Disconnect(context.Background())
				return nil
			}
			client.Disconnect(context.Background())
		}
		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("timeout waiting for MongoDB after %v", timeout)
}

func (m *MongoDBBenchmarker) ResetStats() error {
	// MongoDB doesn't have a direct equivalent to pg_stat_reset()
	// Metrics are tracked via deltas (before/after snapshots)
	return nil
}

// CaptureMetricsBefore takes a snapshot of serverStatus before the workload.
func (m *MongoDBBenchmarker) CaptureMetricsBefore() error {
	ctx := context.Background()
	var result bson.M
	err := m.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&result)
	if err != nil {
		return fmt.Errorf("serverStatus: %w", err)
	}
	m.metricsBefore = result
	return nil
}
