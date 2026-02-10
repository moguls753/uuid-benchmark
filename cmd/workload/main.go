package main

import (
	"context"
	crand "crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Result struct {
	Throughput  float64 `json:"throughput"`
	LatencyP50  int64   `json:"latency_p50_us"`
	LatencyP95  int64   `json:"latency_p95_us"`
	LatencyP99  int64   `json:"latency_p99_us"`
	TotalOps    int     `json:"total_ops"`
	DurationMs  int64   `json:"duration_ms"`
	Errors      int     `json:"errors"`
	InsertOps   int     `json:"insert_ops,omitempty"`
	ReadOps     int     `json:"read_ops,omitempty"`
	UpdateOps   int     `json:"update_ops,omitempty"`
}

// payload is a fixed-size byte slice to simulate realistic row sizes
var payload = make([]byte, 100)

func init() {
	crand.Read(payload)
}

func main() {
	dbType := flag.String("db-type", "", "Database type: mongodb or cassandra")
	op := flag.String("op", "", "Operation: insert, read, update, mixed-insert-heavy, mixed-read-heavy, mixed-balanced")
	keyType := flag.String("key-type", "", "Key type: sequential, uuidv1, uuidv4, uuidv7, ulid, ulid_monotonic")
	numRecords := flag.Int("num-records", 0, "Number of records")
	numOps := flag.Int("num-ops", 0, "Number of operations (for read/update/mixed)")
	batchSize := flag.Int("batch-size", 1, "Batch size for inserts")
	threads := flag.Int("threads", 1, "Number of concurrent threads")
	connString := flag.String("connection-string", "", "Database connection string")
	flag.Parse()

	if *dbType == "" || *op == "" || *keyType == "" {
		log.Fatal("--db-type, --op, and --key-type are required")
	}

	var result *Result
	var err error

	switch *dbType {
	case "mongodb":
		result, err = runMongoDB(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString)
	case "cassandra":
		result, err = runCassandra(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString)
	default:
		log.Fatalf("Unknown db-type: %s", *dbType)
	}

	if err != nil {
		log.Fatalf("Operation failed: %v", err)
	}

	enc := json.NewEncoder(os.Stdout)
	if err := enc.Encode(result); err != nil {
		log.Fatalf("Failed to encode result: %v", err)
	}
}

type keyGenerator struct {
	keyType   string
	counter   *atomic.Int64
	entropy   *ulid.MonotonicEntropy
	entropyMu sync.Mutex
}

func newKeyGenerator(keyType string, counter *atomic.Int64) *keyGenerator {
	kg := &keyGenerator{
		keyType: keyType,
		counter: counter,
	}
	if keyType == "ulid_monotonic" {
		kg.entropy = ulid.Monotonic(crand.Reader, 0)
	}
	return kg
}

// generateMongoKey returns a key suitable for MongoDB _id field.
func (kg *keyGenerator) generateMongoKey() any {
	switch kg.keyType {
	case "sequential":
		return kg.counter.Add(1)
	case "uuidv1":
		u, _ := uuid.NewUUID()
		return bson.Binary{Subtype: 0x04, Data: u[:]}
	case "uuidv4":
		u := uuid.New()
		return bson.Binary{Subtype: 0x04, Data: u[:]}
	case "uuidv7":
		u, _ := uuid.NewV7()
		return bson.Binary{Subtype: 0x04, Data: u[:]}
	case "ulid":
		u := ulid.MustNew(ulid.Now(), crand.Reader)
		return bson.Binary{Subtype: 0x00, Data: u[:]}
	case "ulid_monotonic":
		kg.entropyMu.Lock()
		u := ulid.MustNew(ulid.Now(), kg.entropy)
		kg.entropyMu.Unlock()
		return bson.Binary{Subtype: 0x00, Data: u[:]}
	default:
		return uuid.New()
	}
}

// generateCassandraKey returns a key suitable for Cassandra primary key.
func (kg *keyGenerator) generateCassandraKey() any {
	switch kg.keyType {
	case "sequential":
		return kg.counter.Add(1)
	case "uuidv1":
		u, _ := uuid.NewUUID()
		return gocql.UUID(u)
	case "uuidv4":
		u := uuid.New()
		return gocql.UUID(u)
	case "uuidv7":
		u, _ := uuid.NewV7()
		return gocql.UUID(u)
	case "ulid":
		u := ulid.MustNew(ulid.Now(), crand.Reader)
		return u.Bytes()
	case "ulid_monotonic":
		kg.entropyMu.Lock()
		u := ulid.MustNew(ulid.Now(), kg.entropy)
		kg.entropyMu.Unlock()
		return u.Bytes()
	default:
		u := uuid.New()
		return gocql.UUID(u)
	}
}

func calculatePercentiles(latencies []int64) (p50, p95, p99 int64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	n := len(latencies)
	p50 = latencies[n*50/100]
	p95 = latencies[n*95/100]
	p99 = latencies[n*99/100]
	return
}

func runMongoDB(op, keyType string, numRecords, numOps, batchSize, threads int, connString string) (*Result, error) {
	ctx := context.Background()

	client, err := mongo.Connect(options.Client().ApplyURI(connString))
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("uuid_benchmark")
	coll := db.Collection("bench")

	switch op {
	case "insert":
		return mongoInsert(ctx, coll, keyType, numRecords, batchSize, threads)
	case "read":
		return mongoRead(ctx, coll, keyType, numOps, threads)
	case "update":
		return mongoUpdate(ctx, coll, keyType, numOps, threads)
	case "mixed-insert-heavy":
		return mongoMixed(ctx, coll, keyType, numOps, threads, 90, 10, 0)
	case "mixed-read-heavy":
		return mongoMixed(ctx, coll, keyType, numOps, threads, 10, 90, 0)
	case "mixed-balanced":
		return mongoMixed(ctx, coll, keyType, numOps, threads, 50, 30, 20)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func mongoInsert(ctx context.Context, coll *mongo.Collection, keyType string, numRecords, batchSize, threads int) (*Result, error) {
	var counter atomic.Int64
	recordsPerThread := numRecords / threads
	remainder := numRecords % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		records := recordsPerThread
		if t < remainder {
			records++
		}
		wg.Add(1)
		go func(threadID, records int) {
			defer wg.Done()
			kg := newKeyGenerator(keyType, &counter)
			latencies := make([]int64, 0, (records/batchSize)+1)

			for i := 0; i < records; i += batchSize {
				batchEnd := i + batchSize
				if batchEnd > records {
					batchEnd = records
				}
				docs := make([]any, 0, batchEnd-i)
				for j := i; j < batchEnd; j++ {
					docs = append(docs, bson.D{
						{Key: "_id", Value: kg.generateMongoKey()},
						{Key: "data", Value: payload},
					})
				}

				opStart := time.Now()
				_, err := coll.InsertMany(ctx, docs)
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)

				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, records)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numRecords) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numRecords,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func mongoRead(ctx context.Context, coll *mongo.Collection, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchMongoIDs(ctx, coll, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		offset := t * opsPerThread
		if t < remainder {
			offset += t
		} else {
			offset += remainder
		}
		wg.Add(1)
		go func(threadID, offset, ops int) {
			defer wg.Done()
			latencies := make([]int64, 0, ops)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				var result bson.M
				err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: ids[idx]}}).Decode(&result)
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, offset, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func mongoUpdate(ctx context.Context, coll *mongo.Collection, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchMongoIDs(ctx, coll, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		offset := t * opsPerThread
		if t < remainder {
			offset += t
		} else {
			offset += remainder
		}
		wg.Add(1)
		go func(threadID, offset, ops int) {
			defer wg.Done()
			latencies := make([]int64, 0, ops)
			newPayload := make([]byte, 100)
			crand.Read(newPayload)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				_, err := coll.UpdateOne(ctx,
					bson.D{{Key: "_id", Value: ids[idx]}},
					bson.D{{Key: "$set", Value: bson.D{{Key: "data", Value: newPayload}}}},
				)
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, offset, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func mongoMixed(ctx context.Context, coll *mongo.Collection, keyType string, numOps, threads, insertPct, readPct, updatePct int) (*Result, error) {
	ids, _ := fetchMongoIDs(ctx, coll, 0)
	var idsMu sync.RWMutex

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64
	var totalInserts, totalReads, totalUpdates atomic.Int64
	var counter atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		wg.Add(1)
		go func(threadID, ops int) {
			defer wg.Done()
			kg := newKeyGenerator(keyType, &counter)
			latencies := make([]int64, 0, ops)
			newPayload := make([]byte, 100)
			crand.Read(newPayload)
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

			for i := 0; i < ops; i++ {
				roll := rng.IntN(100)

				if roll < insertPct {
					doc := bson.D{
						{Key: "_id", Value: kg.generateMongoKey()},
						{Key: "data", Value: payload},
					}
					opStart := time.Now()
					res, err := coll.InsertOne(ctx, doc)
					latencies = append(latencies, time.Since(opStart).Microseconds())
					if err != nil {
						totalErrors.Add(1)
					} else {
						idsMu.Lock()
						ids = append(ids, res.InsertedID)
						idsMu.Unlock()
					}
					totalInserts.Add(1)
				} else if roll < insertPct+readPct {
					idsMu.RLock()
					idLen := len(ids)
					var id any
					if idLen > 0 {
						id = ids[rng.IntN(idLen)]
					}
					idsMu.RUnlock()

					if id != nil {
						opStart := time.Now()
						var result bson.M
						err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&result)
						latencies = append(latencies, time.Since(opStart).Microseconds())
						if err != nil {
							totalErrors.Add(1)
						}
					}
					totalReads.Add(1)
				} else {
					idsMu.RLock()
					idLen := len(ids)
					var id any
					if idLen > 0 {
						id = ids[rng.IntN(idLen)]
					}
					idsMu.RUnlock()

					if id != nil {
						opStart := time.Now()
						_, err := coll.UpdateOne(ctx,
							bson.D{{Key: "_id", Value: id}},
							bson.D{{Key: "$set", Value: bson.D{{Key: "data", Value: newPayload}}}},
						)
						latencies = append(latencies, time.Since(opStart).Microseconds())
						if err != nil {
							totalErrors.Add(1)
						}
					}
					totalUpdates.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
		InsertOps:  int(totalInserts.Load()),
		ReadOps:    int(totalReads.Load()),
		UpdateOps:  int(totalUpdates.Load()),
	}, nil
}

func fetchMongoIDs(ctx context.Context, coll *mongo.Collection, limit int) ([]any, error) {
	opts := options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}

	cursor, err := coll.Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var ids []any
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			continue
		}
		ids = append(ids, doc["_id"])
	}
	return ids, nil
}

func runCassandra(op, keyType string, numRecords, numOps, batchSize, threads int, connString string) (*Result, error) {
	cluster := gocql.NewCluster(connString)
	cluster.Keyspace = "uuid_benchmark"
	cluster.Consistency = gocql.LocalOne
	cluster.NumConns = threads
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer session.Close()

	switch op {
	case "insert":
		return cassandraInsert(session, keyType, numRecords, batchSize, threads)
	case "read":
		return cassandraRead(session, keyType, numOps, threads)
	case "update":
		return cassandraUpdate(session, keyType, numOps, threads)
	case "mixed-insert-heavy":
		return cassandraMixed(session, keyType, numOps, threads, 90, 10, 0)
	case "mixed-read-heavy":
		return cassandraMixed(session, keyType, numOps, threads, 10, 90, 0)
	case "mixed-balanced":
		return cassandraMixed(session, keyType, numOps, threads, 50, 30, 20)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

const cassandraInsertQuery = "INSERT INTO bench (id, payload) VALUES (?, ?)"

func cassandraInsert(session *gocql.Session, keyType string, numRecords, batchSize, threads int) (*Result, error) {
	var counter atomic.Int64
	recordsPerThread := numRecords / threads
	remainder := numRecords % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		records := recordsPerThread
		if t < remainder {
			records++
		}
		wg.Add(1)
		go func(threadID, records int) {
			defer wg.Done()
			kg := newKeyGenerator(keyType, &counter)
			latencies := make([]int64, 0, records)

			for i := 0; i < records; i++ {
				key := kg.generateCassandraKey()
				opStart := time.Now()
				err := session.Query(cassandraInsertQuery, key, payload).Exec()
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, records)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numRecords) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numRecords,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func cassandraRead(session *gocql.Session, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchCassandraIDs(session, keyType, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		offset := t * opsPerThread
		if t < remainder {
			offset += t
		} else {
			offset += remainder
		}
		wg.Add(1)
		go func(threadID, offset, ops int) {
			defer wg.Done()
			latencies := make([]int64, 0, ops)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				var readPayload []byte
				err := session.Query("SELECT payload FROM bench WHERE id = ?", ids[idx]).Scan(&readPayload)
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, offset, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func cassandraUpdate(session *gocql.Session, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchCassandraIDs(session, keyType, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		offset := t * opsPerThread
		if t < remainder {
			offset += t
		} else {
			offset += remainder
		}
		wg.Add(1)
		go func(threadID, offset, ops int) {
			defer wg.Done()
			latencies := make([]int64, 0, ops)
			newPayload := make([]byte, 100)
			crand.Read(newPayload)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				err := session.Query("UPDATE bench SET payload = ? WHERE id = ?", newPayload, ids[idx]).Exec()
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					totalErrors.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, offset, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
	}, nil
}

func cassandraMixed(session *gocql.Session, keyType string, numOps, threads, insertPct, readPct, updatePct int) (*Result, error) {
	ids, _ := fetchCassandraIDs(session, keyType, 0)
	var idsMu sync.RWMutex

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64
	var totalInserts, totalReads, totalUpdates atomic.Int64
	var counter atomic.Int64

	start := time.Now()

	for t := 0; t < threads; t++ {
		ops := opsPerThread
		if t < remainder {
			ops++
		}
		wg.Add(1)
		go func(threadID, ops int) {
			defer wg.Done()
			kg := newKeyGenerator(keyType, &counter)
			latencies := make([]int64, 0, ops)
			newPayload := make([]byte, 100)
			crand.Read(newPayload)
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

			for i := 0; i < ops; i++ {
				roll := rng.IntN(100)

				if roll < insertPct {
					key := kg.generateCassandraKey()
					opStart := time.Now()
					err := session.Query(cassandraInsertQuery, key, payload).Exec()
					latencies = append(latencies, time.Since(opStart).Microseconds())
					if err != nil {
						totalErrors.Add(1)
					} else {
						idsMu.Lock()
						ids = append(ids, key)
						idsMu.Unlock()
					}
					totalInserts.Add(1)
				} else if roll < insertPct+readPct {
					idsMu.RLock()
					idLen := len(ids)
					var id any
					if idLen > 0 {
						id = ids[rng.IntN(idLen)]
					}
					idsMu.RUnlock()

					if id != nil {
						opStart := time.Now()
						var readPayload []byte
						err := session.Query("SELECT payload FROM bench WHERE id = ?", id).Scan(&readPayload)
						latencies = append(latencies, time.Since(opStart).Microseconds())
						if err != nil {
							totalErrors.Add(1)
						}
					}
					totalReads.Add(1)
				} else {
					idsMu.RLock()
					idLen := len(ids)
					var id any
					if idLen > 0 {
						id = ids[rng.IntN(idLen)]
					}
					idsMu.RUnlock()

					if id != nil {
						opStart := time.Now()
						err := session.Query("UPDATE bench SET payload = ? WHERE id = ?", newPayload, id).Exec()
						latencies = append(latencies, time.Since(opStart).Microseconds())
						if err != nil {
							totalErrors.Add(1)
						}
					}
					totalUpdates.Add(1)
				}
			}
			allLatencies[threadID] = latencies
		}(t, ops)
	}

	wg.Wait()
	duration := time.Since(start)

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput: float64(numOps) / duration.Seconds(),
		LatencyP50: p50,
		LatencyP95: p95,
		LatencyP99: p99,
		TotalOps:   numOps,
		DurationMs: duration.Milliseconds(),
		Errors:     int(totalErrors.Load()),
		InsertOps:  int(totalInserts.Load()),
		ReadOps:    int(totalReads.Load()),
		UpdateOps:  int(totalUpdates.Load()),
	}, nil
}

func fetchCassandraIDs(session *gocql.Session, keyType string, limit int) ([]any, error) {
	query := "SELECT id FROM bench"
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	iter := session.Query(query).Iter()
	var ids []any

	switch keyType {
	case "sequential":
		var id int64
		for iter.Scan(&id) {
			ids = append(ids, id)
		}
	case "uuidv1", "uuidv4", "uuidv7":
		var id gocql.UUID
		for iter.Scan(&id) {
			ids = append(ids, id)
		}
	case "ulid", "ulid_monotonic":
		var id []byte
		for iter.Scan(&id) {
			idCopy := make([]byte, len(id))
			copy(idCopy, id)
			ids = append(ids, idCopy)
		}
	}

	if err := iter.Close(); err != nil {
		return ids, err
	}
	return ids, nil
}
