package main

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"sync"
	"regexp"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql"
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
var payload = make([]byte, 1024)

func init() {
	crand.Read(payload)
}

func main() {
	dbType := flag.String("db-type", "", "Database type: mongodb or cassandra")
	op := flag.String("op", "", "Operation: insert, read, update, mixed")
	keyType := flag.String("key-type", "", "Key type: sequential, uuidv1, uuidv4, uuidv7, ulid, ulid_monotonic")
	numRecords := flag.Int("num-records", 0, "Number of records")
	numOps := flag.Int("num-ops", 0, "Number of operations (for read/update/mixed)")
	batchSize := flag.Int("batch-size", 1, "Batch size for inserts")
	threads := flag.Int("threads", 1, "Number of concurrent threads")
	connString := flag.String("connection-string", "", "Database connection string")
	insertPct := flag.Int("insert-pct", 0, "Insert percentage for mixed workload")
	readPct := flag.Int("read-pct", 0, "Read percentage for mixed workload")
	updatePct := flag.Int("update-pct", 0, "Update percentage for mixed workload")
	tableName := flag.String("table-name", "bench", "Table/collection name")
	numBuckets := flag.Int("num-buckets", 1000, "Number of Cassandra partition buckets")
	consistency := flag.String("consistency", "local_one", "CQL consistency level (Cassandra only): one, local_one, local_quorum, quorum")
	flag.Parse()

	if *dbType == "" || *op == "" || *keyType == "" {
		log.Fatal("--db-type, --op, and --key-type are required")
	}

	var result *Result
	var err error

	switch *dbType {
	case "mongodb":
		result, err = runMongoDB(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString, *insertPct, *readPct, *updatePct)
	case "cassandra":
		result, err = runCassandra(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString, *insertPct, *readPct, *updatePct, *numBuckets, *consistency)
	case "mysql":
		result, err = runMySQL(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString, *insertPct, *readPct, *updatePct, *tableName)
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
	case "objectid":
		return bson.NewObjectID()
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

// generateMySQLKey returns a []byte key suitable for MySQL BINARY(16) columns.
// Returns nil for sequential (AUTO_INCREMENT handled by MySQL).
func (kg *keyGenerator) generateMySQLKey() []byte {
	switch kg.keyType {
	case "sequential":
		return nil // AUTO_INCREMENT
	case "uuidv1":
		u, _ := uuid.NewUUID()
		return u[:]
	case "uuidv4":
		u := uuid.New()
		return u[:]
	case "uuidv7":
		u, _ := uuid.NewV7()
		return u[:]
	case "ulid":
		u := ulid.MustNew(ulid.Now(), crand.Reader)
		return u[:]
	case "ulid_monotonic":
		kg.entropyMu.Lock()
		u := ulid.MustNew(ulid.Now(), kg.entropy)
		kg.entropyMu.Unlock()
		return u[:]
	default:
		u := uuid.New()
		return u[:]
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

func runMongoDB(op, keyType string, numRecords, numOps, batchSize, threads int, connString string, insertPct, readPct, updatePct int) (*Result, error) {
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
	case "mixed":
		return mongoMixed(ctx, coll, keyType, numOps, threads, insertPct, readPct, updatePct)
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
			newPayload := make([]byte, 1024)
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
	ids, _ := fetchMongoIDs(ctx, coll, numOps)
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
			newPayload := make([]byte, 1024)
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

var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func runMySQL(op, keyType string, numRecords, numOps, batchSize, threads int, connString string, insertPct, readPct, updatePct int, tableName string) (*Result, error) {
	if !validTableName.MatchString(tableName) {
		return nil, fmt.Errorf("invalid table name: %s", tableName)
	}

	db, err := sql.Open("mysql", connString)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(threads + 5)
	db.SetMaxIdleConns(threads)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping: %w", err)
	}

	switch op {
	case "insert":
		return mysqlInsert(db, tableName, keyType, numRecords, batchSize, threads)
	case "read":
		return mysqlRead(db, tableName, keyType, numOps, threads)
	case "update":
		return mysqlUpdate(db, tableName, keyType, numOps, threads)
	case "mixed":
		return mysqlMixed(db, tableName, keyType, numOps, threads, insertPct, readPct, updatePct)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func mysqlInsert(db *sql.DB, tableName, keyType string, numRecords, batchSize, threads int) (*Result, error) {
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
				currentBatch := batchEnd - i

				opStart := time.Now()
				var err error
				if keyType == "sequential" {
					err = mysqlInsertBatchSequential(db, tableName, currentBatch)
				} else {
					keys := make([][]byte, currentBatch)
					for j := 0; j < currentBatch; j++ {
						keys[j] = kg.generateMySQLKey()
					}
					err = mysqlInsertBatchUUID(db, tableName, keys)
				}
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

func mysqlInsertBatchSequential(db *sql.DB, tableName string, count int) error {
	query := fmt.Sprintf("INSERT INTO %s (data) VALUES ", tableName)
	vals := make([]any, 0, count)
	for i := 0; i < count; i++ {
		if i > 0 {
			query += ","
		}
		query += "(?)"
		vals = append(vals, payload)
	}
	_, err := db.Exec(query, vals...)
	return err
}

func mysqlInsertBatchUUID(db *sql.DB, tableName string, keys [][]byte) error {
	query := fmt.Sprintf("INSERT INTO %s (id, data) VALUES ", tableName)
	vals := make([]any, 0, len(keys)*2)
	for i, key := range keys {
		if i > 0 {
			query += ","
		}
		query += "(?,?)"
		vals = append(vals, key, payload)
	}
	_, err := db.Exec(query, vals...)
	return err
}

func fetchMySQLIDs(db *sql.DB, tableName, keyType string, limit int) ([]any, error) {
	query := fmt.Sprintf("SELECT id FROM %s", tableName)
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []any
	for rows.Next() {
		if keyType == "sequential" {
			var id int64
			if err := rows.Scan(&id); err != nil {
				continue
			}
			ids = append(ids, id)
		} else {
			var id []byte
			if err := rows.Scan(&id); err != nil {
				continue
			}
			idCopy := make([]byte, len(id))
			copy(idCopy, id)
			ids = append(ids, idCopy)
		}
	}
	return ids, rows.Err()
}

func mysqlRead(db *sql.DB, tableName, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchMySQLIDs(db, tableName, keyType, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	query := fmt.Sprintf("SELECT id, data FROM %s WHERE id = ?", tableName)

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
				row := db.QueryRow(query, ids[idx])
				var readID any
				var readData []byte
				err := row.Scan(&readID, &readData)
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

func mysqlUpdate(db *sql.DB, tableName, keyType string, numOps, threads int) (*Result, error) {
	ids, err := fetchMySQLIDs(db, tableName, keyType, numOps)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}

	query := fmt.Sprintf("UPDATE %s SET data = ? WHERE id = ?", tableName)

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
			newPayload := make([]byte, 1024)
			crand.Read(newPayload)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				_, err := db.Exec(query, newPayload, ids[idx])
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

func mysqlMixed(db *sql.DB, tableName, keyType string, numOps, threads, insertPct, readPct, updatePct int) (*Result, error) {
	ids, _ := fetchMySQLIDs(db, tableName, keyType, numOps)
	var idsMu sync.RWMutex

	insertSeqQuery := fmt.Sprintf("INSERT INTO %s (data) VALUES (?)", tableName)
	insertUUIDQuery := fmt.Sprintf("INSERT INTO %s (id, data) VALUES (?, ?)", tableName)
	readQuery := fmt.Sprintf("SELECT id, data FROM %s WHERE id = ?", tableName)
	updateQuery := fmt.Sprintf("UPDATE %s SET data = ? WHERE id = ?", tableName)

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
			newPayload := make([]byte, 1024)
			crand.Read(newPayload)
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

			for i := 0; i < ops; i++ {
				roll := rng.IntN(100)

				if roll < insertPct {
					key := kg.generateMySQLKey()
					opStart := time.Now()
					var err error
					if keyType == "sequential" {
						_, err = db.Exec(insertSeqQuery, payload)
					} else {
						_, err = db.Exec(insertUUIDQuery, key, payload)
					}
					latencies = append(latencies, time.Since(opStart).Microseconds())
					if err != nil {
						totalErrors.Add(1)
					} else if key != nil {
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
						row := db.QueryRow(readQuery, id)
						var readID any
						var readData []byte
						err := row.Scan(&readID, &readData)
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
						_, err := db.Exec(updateQuery, newPayload, id)
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

// parseContactPoints splits a comma-separated list of Cassandra contact points
// into a slice suitable for gocql.NewCluster. Whitespace around each entry is
// trimmed and empty entries are dropped (so stray leading, trailing, or
// doubled commas are tolerated). An empty input yields an empty slice; the
// caller must validate before passing the result to gocql.
func parseContactPoints(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// parseConsistency mirrors the orchestrator's
// internal/benchmark/cassandra.parseConsistency. Re-implemented locally
// because cmd/workload is a standalone binary that does not import the
// orchestrator's cassandra package. The accepted string values match
// internal/cluster.Consistency constants.
func parseConsistency(s string) gocql.Consistency {
	switch s {
	case "one":
		return gocql.One
	case "local_one":
		return gocql.LocalOne
	case "local_quorum":
		return gocql.LocalQuorum
	case "quorum":
		return gocql.Quorum
	default:
		return gocql.LocalOne
	}
}

func runCassandra(op, keyType string, numRecords, numOps, batchSize, threads int, connString string, insertPct, readPct, updatePct, numBuckets int, consistency string) (*Result, error) {
	points := parseContactPoints(connString)
	if len(points) == 0 {
		return nil, fmt.Errorf("no Cassandra contact points provided")
	}
	cluster := gocql.NewCluster(points...)
	cluster.Keyspace = "uuid_benchmark"
	cluster.Consistency = parseConsistency(consistency)
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
		return cassandraInsert(session, keyType, numRecords, batchSize, threads, numBuckets)
	case "read":
		return cassandraRead(session, keyType, numOps, threads, numBuckets)
	case "update":
		return cassandraUpdate(session, keyType, numOps, threads, numBuckets)
	case "mixed":
		return cassandraMixed(session, keyType, numOps, threads, insertPct, readPct, updatePct, numBuckets)
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

// cassandraBucketQuery executes the per-bucket SELECT used by
// fetchCassandraIDs. Extracted so the bucket-iteration logic in
// fetchIDsAcrossBuckets is testable without a live gocql session — tests
// pass a stub that returns synthetic per-bucket id slices.
type cassandraBucketQuery func(bucket, perBucketLimit int) ([]any, error)

const (
	cassandraInsertQuery = "INSERT INTO bench (bucket, id, payload) VALUES (?, ?, ?)"
	cassandraReadQuery   = "SELECT payload FROM bench WHERE bucket = ? AND id = ?"
	cassandraUpdateQuery = "UPDATE bench SET payload = ? WHERE bucket = ? AND id = ?"
)

// idAsBytes returns a stable byte representation of an id for hashing.
// Handles the id types produced by the various UUID/ULID/sequential generators
// in this workload binary.
//
// Note: hot-path callers should prefer bucketForIDValue, which folds this
// type-switch into the hash loop and avoids the per-call slice allocation
// implicit in this function (returning a slice over a local array forces
// heap escape).
func idAsBytes(id any) []byte {
	switch v := id.(type) {
	case gocql.UUID:
		b := [16]byte(v)
		return b[:]
	case []byte:
		return v
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	default:
		// Defensive fallback: stringify. Stable per-type but suboptimal.
		return []byte(fmt.Sprintf("%v", v))
	}
}

// bucketForID returns a stable bucket assignment for the given id bytes.
// Used to spread data across N Cassandra partitions while keeping id-as-
// clustering-column behavior from the thesis intact. Deterministic and
// approximately uniform for non-adversarial input.
//
// Inlined FNV-1a (zero allocation) since this runs once per row on the hot
// path; the stdlib hash/fnv.New32a() escapes its hasher to the heap.
//
// Panics on n <= 0: a misconfigured numBuckets would silently route every row
// to bucket 0 and recreate the thesis's single-partition design, invalidating
// the entire multi-node experiment. Loud failure is the only safe behavior.
func bucketForID(id []byte, n int) int {
	if n <= 0 {
		panic("bucketForID: n must be >= 1")
	}
	const offset32 = 2166136261
	const prime32 = 16777619
	var h uint32 = offset32
	for _, b := range id {
		h ^= uint32(b)
		h *= prime32
	}
	return int(h % uint32(n))
}

// bucketForIDValue is the typed, zero-allocation companion to bucketForID for
// hot-path use. It accepts the id types produced by generateCassandraKey
// (gocql.UUID, []byte, int64) and computes the FNV-1a hash directly over the
// id's bytes without materializing an intermediate []byte (which would force
// a heap allocation per call — see idAsBytes for that pattern).
//
// At 100M-record benchmark scale this saves ~1.6GB of cumulative allocations
// on the insert path alone, eliminating GC-induced p99 latency jitter that
// would otherwise contaminate the throughput measurements.
//
// Equivalent in output to bucketForID(idAsBytes(id), n) for any supported type;
// see TestBucketForIDValueMatchesBucketForID for the contract.
//
// Panics on n <= 0 for the same reason as bucketForID.
func bucketForIDValue(id any, n int) int {
	if n <= 0 {
		panic("bucketForIDValue: n must be >= 1")
	}
	const offset32 = 2166136261
	const prime32 = 16777619
	var h uint32 = offset32
	switch v := id.(type) {
	case gocql.UUID:
		for i := 0; i < 16; i++ {
			h ^= uint32(v[i])
			h *= prime32
		}
	case []byte:
		for _, b := range v {
			h ^= uint32(b)
			h *= prime32
		}
	case int64:
		u := uint64(v)
		for i := 7; i >= 0; i-- {
			h ^= uint32(byte(u >> (i * 8)))
			h *= prime32
		}
	default:
		// Defensive fallback (unreachable for generateCassandraKey return types).
		for _, b := range []byte(fmt.Sprintf("%v", v)) {
			h ^= uint32(b)
			h *= prime32
		}
	}
	return int(h % uint32(n))
}

func cassandraInsert(session *gocql.Session, keyType string, numRecords, batchSize, threads, numBuckets int) (*Result, error) {
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

				batch := session.NewBatch(gocql.UnloggedBatch)
				for j := i; j < batchEnd; j++ {
					key := kg.generateCassandraKey()
					bucket := bucketForIDValue(key, numBuckets)
					batch.Query(cassandraInsertQuery, bucket, key, payload)
				}

				opStart := time.Now()
				err := session.ExecuteBatch(batch)
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

func cassandraRead(session *gocql.Session, keyType string, numOps, threads, numBuckets int) (*Result, error) {
	ids, err := fetchCassandraIDs(session, keyType, numOps, numBuckets)
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
				bucket := bucketForIDValue(ids[idx], numBuckets)
				err := session.Query(cassandraReadQuery, bucket, ids[idx]).Scan(&readPayload)
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

func cassandraUpdate(session *gocql.Session, keyType string, numOps, threads, numBuckets int) (*Result, error) {
	ids, err := fetchCassandraIDs(session, keyType, numOps, numBuckets)
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
			newPayload := make([]byte, 1024)
			crand.Read(newPayload)

			for i := 0; i < ops; i++ {
				idx := (offset + i) % len(ids)
				opStart := time.Now()
				bucket := bucketForIDValue(ids[idx], numBuckets)
				err := session.Query(cassandraUpdateQuery, newPayload, bucket, ids[idx]).Exec()
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

func cassandraMixed(session *gocql.Session, keyType string, numOps, threads, insertPct, readPct, updatePct, numBuckets int) (*Result, error) {
	ids, err := fetchCassandraIDs(session, keyType, numOps, numBuckets)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}
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
			newPayload := make([]byte, 1024)
			crand.Read(newPayload)
			rng := rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))

			for i := 0; i < ops; i++ {
				roll := rng.IntN(100)

				if roll < insertPct {
					key := kg.generateCassandraKey()
					bucket := bucketForIDValue(key, numBuckets)
					opStart := time.Now()
					err := session.Query(cassandraInsertQuery, bucket, key, payload).Exec()
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
						bucket := bucketForIDValue(id, numBuckets)
						err := session.Query(cassandraReadQuery, bucket, id).Scan(&readPayload)
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
						bucket := bucketForIDValue(id, numBuckets)
						err := session.Query(cassandraUpdateQuery, newPayload, bucket, id).Exec()
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

// fetchCassandraIDs samples up to `limit` ids spread across all numBuckets
// partitions via `PER PARTITION LIMIT`. The previous `SELECT id FROM bench
// LIMIT N` form returned ids in Murmur3 token order, which concentrates the
// sample on whatever 1-2 partitions sort first by token — at scale every
// read/update workload ended up hitting the same few partitions, saturating
// the key cache at 1.0 regardless of UUID type and erasing the read-
// amplification signal the multi-node extension is designed to measure.
//
// Per-bucket query is structured behind cassandraBucketQuery so the
// bucket-iteration logic is unit-testable without a live gocql session —
// see fetchIDsAcrossBuckets.
func fetchCassandraIDs(session *gocql.Session, keyType string, limit, numBuckets int) ([]any, error) {
	if numBuckets <= 0 {
		return nil, fmt.Errorf("fetchCassandraIDs: numBuckets must be >= 1, got %d", numBuckets)
	}
	query := func(bucket, perBucketLimit int) ([]any, error) {
		return queryBucketIDs(session, keyType, bucket, perBucketLimit)
	}
	ids, err := fetchIDsAcrossBuckets(query, limit, numBuckets)
	if err != nil {
		return ids, err
	}
	if len(ids) == 0 {
		return nil, fmt.Errorf("fetchCassandraIDs: no rows returned from bench table (limit=%d, keyType=%s, numBuckets=%d); the table appears empty, which likely means inserts did not complete", limit, keyType, numBuckets)
	}
	return ids, nil
}

// fetchIDsAcrossBuckets iterates buckets 0..numBuckets-1 calling query for
// each, accumulating up to `limit` ids total. perBucket is sized by ceil
// division so the sample reaches `limit` even when some buckets are sparse
// — we stop as soon as we've accumulated `limit` ids regardless of which
// bucket we're on.
//
// Errors from any per-bucket query are propagated immediately (no partial
// returns) — a transient gocql error mid-iteration would otherwise return a
// silently-truncated sample that biases the downstream workload.
func fetchIDsAcrossBuckets(query cassandraBucketQuery, limit, numBuckets int) ([]any, error) {
	if limit <= 0 {
		return nil, nil
	}
	perBucket := (limit + numBuckets - 1) / numBuckets
	if perBucket < 1 {
		perBucket = 1
	}
	ids := make([]any, 0, limit)
	for b := 0; b < numBuckets && len(ids) < limit; b++ {
		rows, err := query(b, perBucket)
		if err != nil {
			return ids, err
		}
		for _, id := range rows {
			if len(ids) >= limit {
				break
			}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// queryBucketIDs runs the `SELECT id FROM bench WHERE bucket = ? PER
// PARTITION LIMIT ?` query for a single bucket and scans the result into a
// typed []any sized by perBucketLimit. The PER PARTITION LIMIT (rather than
// a top-level LIMIT) is important: it caps the per-partition scan, which
// matters because callers iterate buckets in sequence to spread the sample.
func queryBucketIDs(session *gocql.Session, keyType string, bucket, perBucketLimit int) ([]any, error) {
	iter := session.Query("SELECT id FROM bench WHERE bucket = ? PER PARTITION LIMIT ?", bucket, perBucketLimit).Iter()
	ids := make([]any, 0, perBucketLimit)

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
	default:
		_ = iter.Close()
		return nil, fmt.Errorf("queryBucketIDs: unknown keyType %q", keyType)
	}

	if err := iter.Close(); err != nil {
		return ids, err
	}
	return ids, nil
}
