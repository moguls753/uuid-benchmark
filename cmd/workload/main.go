package main

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	Throughput float64 `json:"throughput"`
	LatencyP50 int64   `json:"latency_p50_us"`
	LatencyP95 int64   `json:"latency_p95_us"`
	LatencyP99 int64   `json:"latency_p99_us"`
	TotalOps   int     `json:"total_ops"`
	DurationMs int64   `json:"duration_ms"`
	Errors     int     `json:"errors"`
	// NotFound counts operations whose query succeeded but returned no row.
	// Tracked separately from Errors because the read target ids are drawn
	// during the insert (see sampleIndices): a row that never made it into
	// the table returns fast and would otherwise inflate throughput while
	// looking like a clean run.
	NotFound int `json:"not_found"`
	// FetchMs is the time spent fetching target ids from the database. It is
	// zero whenever the ids come from an id file, which is what keeps the
	// runner's I/O window free of sampling traffic.
	FetchMs int64 `json:"fetch_ms"`
	// IDFileSHA256 fingerprints the read set an insert handed to the next
	// phase. Without it the target set is unverifiable afterwards: the file
	// lives in a container or a temp dir and is gone by the time anyone
	// looks at the CSV.
	IDFileSHA256 string `json:"id_file_sha256,omitempty"`
	// ErrorSamples holds a few distinct error strings from the run. Counting
	// failures without keeping their text leaves "why did 3000 rows not make
	// it" unanswerable once the container is gone, which is the position the
	// first campaign run put us in.
	ErrorSamples []string `json:"error_samples,omitempty"`
	InsertOps    int      `json:"insert_ops,omitempty"`
	ReadOps      int      `json:"read_ops,omitempty"`
	UpdateOps    int      `json:"update_ops,omitempty"`
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
	idFile := flag.String("id-file", "", "Target-id file (Cassandra only). On --op=insert the run samples --sample-size ids uniformly over insert order, shuffles them and writes them here. On --op=read/update the ids are read from here instead of being fetched from the database.")
	sampleSize := flag.Int("sample-size", 0, "Number of ids to sample during insert (requires --id-file on --op=insert)")
	sampleSeed := flag.Int64("sample-seed", 0, "Seed for the id sample and its shuffle (requires --id-file on --op=insert)")
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
		result, err = runCassandra(*op, *keyType, *numRecords, *numOps, *batchSize, *threads, *connString, *insertPct, *readPct, *updatePct, *numBuckets, *consistency, *idFile, *sampleSize, *sampleSeed)
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

func runCassandra(op, keyType string, numRecords, numOps, batchSize, threads int, connString string, insertPct, readPct, updatePct, numBuckets int, consistency, idFile string, sampleSize int, sampleSeed int64) (*Result, error) {
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
		return cassandraInsert(session, keyType, numRecords, batchSize, threads, numBuckets, idFile, sampleSize, sampleSeed)
	case "read":
		return cassandraRead(session, keyType, numOps, threads, numBuckets, idFile)
	case "update":
		return cassandraUpdate(session, keyType, numOps, threads, numBuckets, idFile)
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

func cassandraInsert(session *gocql.Session, keyType string, numRecords, batchSize, threads, numBuckets int, idFile string, sampleSize int, sampleSeed int64) (*Result, error) {
	var counter atomic.Int64
	recordsPerThread := numRecords / threads
	remainder := numRecords % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	sampled := make([][]any, threads)
	errorSamples := make([][]string, threads)
	var totalErrors atomic.Int64

	// One draw over all rows, then split by writer range. Drawing per thread
	// instead would give rows unequal inclusion probability whenever the
	// threads own different numbers of rows, which is what numRecords not
	// dividing by threads produces.
	var perThreadWant [][]int
	if idFile != "" {
		perThreadWant = splitSampleByThread(sampleIndices(sampleSize, numRecords, sampleSeed), recordsPerThread, remainder, threads)
	}

	start := time.Now()

	for t := 0; t < threads; t++ {
		records := recordsPerThread
		if t < remainder {
			records++
		}
		var wantIdx []int
		if perThreadWant != nil {
			wantIdx = perThreadWant[t]
		}
		wg.Add(1)
		go func(threadID, records int, wantIdx []int) {
			defer wg.Done()
			// One generator per writer, unchanged. Only ulid_monotonic carries
			// state here, so with more than one writer that type becomes as
			// many interleaved monotonic streams as there are writers rather
			// than one. The dilution is confined to keys sharing a millisecond
			// timestamp prefix and does not widen the SSTable key ranges the
			// read benchmark is about; keeping it identical across every
			// scenario and both samplers matters more than removing it.
			kg := newKeyGenerator(keyType, &counter)
			latencies := make([]int64, 0, (records/batchSize)+1)

			// wantIdx is ascending and so is j below, so one cursor replaces a
			// per-row set lookup across all numRecords rows.
			var wantCursor int
			if len(wantIdx) > 0 {
				sampled[threadID] = make([]any, 0, len(wantIdx))
			}

			for i := 0; i < records; i += batchSize {
				batchEnd := i + batchSize
				if batchEnd > records {
					batchEnd = records
				}

				batch := session.NewBatch(gocql.UnloggedBatch)
				var staged []any
				for j := i; j < batchEnd; j++ {
					key := kg.generateCassandraKey()
					if wantCursor < len(wantIdx) && wantIdx[wantCursor] == j {
						staged = append(staged, key)
						wantCursor++
					}
					bucket := bucketForIDValue(key, numBuckets)
					batch.Query(cassandraInsertQuery, bucket, key, payload)
				}

				opStart := time.Now()
				err := session.ExecuteBatch(batch)
				latUS := time.Since(opStart).Microseconds()
				latencies = append(latencies, latUS)
				if err != nil {
					// Count rows, not batches: a failed batch loses every row
					// in it, and reporting one failure out of numRecords would
					// describe a dead run as 99.9 % healthy.
					totalErrors.Add(int64(batchEnd - i))
					errorSamples[threadID] = keepDistinct(errorSamples[threadID], err.Error())
					continue
				}
				// Only rows that were actually written become read targets.
				// Otherwise a failed batch would seed the read set with rows
				// that do not exist: fast misses on read, and on update a
				// silent upsert that creates them inside the measured phase.
				sampled[threadID] = append(sampled[threadID], staged...)
			}
			allLatencies[threadID] = latencies
		}(t, records, wantIdx)
	}

	wg.Wait()
	duration := time.Since(start)

	// Written after the timer stops: the read set is bookkeeping for the next
	// phase, not part of the measured insert.
	var idFileSum string
	if idFile != "" {
		ids := make([]any, 0, sampleSize)
		for _, part := range sampled {
			ids = append(ids, part...)
		}
		shuffleIDs(ids, sampleSeed)
		sum, err := writeIDFile(idFile, keyType, sampleSeed, ids)
		if err != nil {
			return nil, fmt.Errorf("write id file: %w", err)
		}
		idFileSum = sum
	}

	var merged []int64
	for _, l := range allLatencies {
		merged = append(merged, l...)
	}
	var samples []string
	for _, part := range errorSamples {
		for _, text := range part {
			samples = keepDistinct(samples, text)
		}
	}

	p50, p95, p99 := calculatePercentiles(merged)

	return &Result{
		Throughput:   float64(numRecords) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		TotalOps:     numRecords,
		DurationMs:   duration.Milliseconds(),
		Errors:       int(totalErrors.Load()),
		ErrorSamples: samples,
		IDFileSHA256: idFileSum,
	}, nil
}

func cassandraRead(session *gocql.Session, keyType string, numOps, threads, numBuckets int, idFile string) (*Result, error) {
	ids, fetchDur, readSetSum, err := targetIDs(session, keyType, numOps, numBuckets, idFile)
	if err != nil {
		return nil, err
	}
	if idFile != "" {
		// The read set is the workload. If the insert lost rows, fewer
		// operations run and attempted drops below num-ops, which is visible
		// downstream instead of being cycled away by a modulo.
		numOps = len(ids)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64
	var totalNotFound atomic.Int64

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
					if errors.Is(err, gocql.ErrNotFound) {
						totalNotFound.Add(1)
					} else {
						totalErrors.Add(1)
					}
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
		Throughput:   float64(numOps) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		TotalOps:     numOps,
		DurationMs:   duration.Milliseconds(),
		Errors:       int(totalErrors.Load()),
		NotFound:     int(totalNotFound.Load()),
		FetchMs:      fetchDur.Milliseconds(),
		IDFileSHA256: readSetSum,
	}, nil
}

func cassandraUpdate(session *gocql.Session, keyType string, numOps, threads, numBuckets int, idFile string) (*Result, error) {
	ids, fetchDur, readSetSum, err := targetIDs(session, keyType, numOps, numBuckets, idFile)
	if err != nil {
		return nil, err
	}
	if idFile != "" {
		// The read set is the workload. If the insert lost rows, fewer
		// operations run and attempted drops below num-ops, which is visible
		// downstream instead of being cycled away by a modulo.
		numOps = len(ids)
	}

	opsPerThread := numOps / threads
	remainder := numOps % threads

	var wg sync.WaitGroup
	allLatencies := make([][]int64, threads)
	var totalErrors atomic.Int64
	var totalNotFound atomic.Int64

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
					if errors.Is(err, gocql.ErrNotFound) {
						totalNotFound.Add(1)
					} else {
						totalErrors.Add(1)
					}
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
		Throughput:   float64(numOps) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		TotalOps:     numOps,
		DurationMs:   duration.Milliseconds(),
		Errors:       int(totalErrors.Load()),
		NotFound:     int(totalNotFound.Load()),
		FetchMs:      fetchDur.Milliseconds(),
		IDFileSHA256: readSetSum,
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

// keepDistinct appends text unless it is already present, and stops growing at
// maxErrorSamples. A run that fails the same way ten thousand times needs to
// say so once, and a run that fails in several ways needs to show each.
func keepDistinct(seen []string, text string) []string {
	if len(seen) >= maxErrorSamples {
		return seen
	}
	for _, existing := range seen {
		if existing == text {
			return seen
		}
	}
	return append(seen, text)
}

const maxErrorSamples = 5

// targetIDs returns the read/update target set and the time spent asking the
// database for it. With an id file the set was drawn during the insert
// (sampleIndices) and no query runs at all, so the fetch duration is zero by
// construction — that is what keeps the runner's I/O window free of sampling
// traffic and removes the pre-warming of exactly the rows about to be
// measured. Without an id file the legacy partition-head fetch runs, which is
// the bridge arm that quantifies what that sampling was worth.
//
// On the id-file path the returned set also defines how many operations run:
// the caller uses len(ids) rather than the requested numOps. A short file means
// the insert lost rows to failed batches, and cycling the survivors with a
// modulo would hide that behind a full-looking operation count. Letting
// attempted fall below num-ops instead surfaces it in the exported counters,
// where the protocol's validity rule catches it.
// The returned digest is computed by the reader over the bytes it actually
// loaded. The runner compares it against the digest the insert reported, so a
// read set that was replaced or edited between the phases is caught instead of
// producing plausible numbers against the wrong rows.
func targetIDs(session *gocql.Session, keyType string, numOps, numBuckets int, idFile string) ([]any, time.Duration, string, error) {
	if idFile != "" {
		ids, sum, err := loadIDFile(idFile, keyType)
		if err != nil {
			return nil, 0, "", fmt.Errorf("load id file: %w", err)
		}
		return ids, 0, sum, nil
	}
	start := time.Now()
	ids, err := fetchCassandraIDs(session, keyType, numOps, numBuckets)
	if err != nil {
		return nil, 0, "", fmt.Errorf("fetch ids: %w", err)
	}
	return ids, time.Since(start), "", nil
}

// threadRange returns the [start, end) slice of insert order owned by writer
// thread t. It mirrors the record split in cassandraInsert, where the first
// `remainder` threads take one extra row.
func threadRange(t, recordsPerThread, remainder int) (int, int) {
	start := t*recordsPerThread + min(t, remainder)
	size := recordsPerThread
	if t < remainder {
		size++
	}
	return start, start + size
}

// splitSampleByThread maps globally drawn ascending record indices onto the
// writer threads that will generate them, rebased to each thread's own
// counter. The draw happens once over all rows and is split afterwards so that
// every row has the same inclusion probability; drawing a fixed share per
// thread would not, because the threads can own different numbers of rows.
func splitSampleByThread(global []int, recordsPerThread, remainder, threads int) [][]int {
	out := make([][]int, threads)
	cursor := 0
	for t := 0; t < threads; t++ {
		start, end := threadRange(t, recordsPerThread, remainder)
		for cursor < len(global) && global[cursor] < end {
			out[t] = append(out[t], global[cursor]-start)
			cursor++
		}
	}
	return out
}

// sampleIndices draws k distinct indices uniformly without replacement from
// [0, n) and returns them ascending. Rejection sampling keeps this allocation-
// light for the production shape (k is well under 1 % of n), and the Perm
// fallback covers the small-dataset smoke tests where k approaches n and
// rejection would thrash.
func sampleIndices(k, n int, seed int64) []int {
	if k <= 0 || n <= 0 {
		return nil
	}
	if k > n {
		k = n
	}
	rng := rand.New(rand.NewPCG(uint64(seed), idSampleStream))
	if k*2 > n {
		idx := rng.Perm(n)[:k]
		sort.Ints(idx)
		return idx
	}
	seen := make(map[int]struct{}, k)
	out := make([]int, 0, k)
	for len(out) < k {
		i := rng.IntN(n)
		if _, dup := seen[i]; dup {
			continue
		}
		seen[i] = struct{}{}
		out = append(out, i)
	}
	sort.Ints(out)
	return out
}

// shuffleIDs randomises the read order in place. Without it the read set would
// be traversed in insert order, which for the time-ordered key types is also
// disk order: a near-sequential walk that keeps one SSTable's index and bloom
// filter hot at a time, while UUIDv4 would jump between all of them. That is
// the same type-dependent advantage the sampling change removes, one level
// down, so both have to go.
func shuffleIDs(ids []any, seed int64) {
	rng := rand.New(rand.NewPCG(uint64(seed), idShuffleStream))
	rng.Shuffle(len(ids), func(i, j int) { ids[i], ids[j] = ids[j], ids[i] })
}

// The two PCG streams keep the sample draw and the shuffle independent even
// though both derive from the same run seed.
const (
	idSampleStream  = 0x9e3779b97f4a7c15
	idShuffleStream = 0xc2b2ae3d27d4eb4f
)

// writeIDFile persists the read set. Line 1 is "<key type> <count> <seed>",
// every following line one id. Text rather than a binary encoding: the file is
// small (one line per operation), has to survive a docker cp into the
// container, and is worth being able to read by eye when a run looks wrong.
// The header count catches a truncated file and the seed ties the set back to
// the run manifest.
// It returns the SHA-256 of the file content so the run manifest can pin
// which read set a measurement actually used.
func writeIDFile(path, keyType string, seed int64, ids []any) (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %d %d\n", keyType, len(ids), seed)
	for _, id := range ids {
		text, err := formatID(id)
		if err != nil {
			return "", err
		}
		b.WriteString(text)
		b.WriteByte('\n')
	}
	content := []byte(b.String())
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return "", err
	}
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:]), nil
}

func formatID(id any) (string, error) {
	switch v := id.(type) {
	case int64:
		return strconv.FormatInt(v, 10), nil
	case gocql.UUID:
		return v.String(), nil
	case []byte:
		return hex.EncodeToString(v), nil
	default:
		return "", fmt.Errorf("formatID: unsupported id type %T", id)
	}
}

// loadIDFile reads back what writeIDFile wrote and refuses anything that does
// not match this run: a foreign key type (a file from a different container
// generation, whose every lookup would miss) or a body shorter than the header
// promises (a write that was cut off). Both would otherwise produce a run that
// looks healthy in every exported number.
func loadIDFile(path, keyType string) ([]any, string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	digest := sha256.Sum256(raw)
	sum := hex.EncodeToString(digest[:])
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) < 2 {
		return nil, sum, fmt.Errorf("id file %s holds no ids", path)
	}
	header := strings.Fields(lines[0])
	if len(header) != 3 {
		return nil, sum, fmt.Errorf("id file %s has a malformed header %q", path, lines[0])
	}
	if header[0] != keyType {
		return nil, sum, fmt.Errorf("id file %s was written for key type %q, this run is %q", path, header[0], keyType)
	}
	want, err := strconv.Atoi(header[1])
	if err != nil {
		return nil, sum, fmt.Errorf("id file %s has a malformed id count %q", path, header[1])
	}
	if got := len(lines) - 1; got != want {
		return nil, sum, fmt.Errorf("id file %s is truncated: header promises %d ids, body holds %d", path, want, got)
	}
	ids := make([]any, 0, len(lines)-1)
	for i, line := range lines[1:] {
		id, err := parseID(keyType, line)
		if err != nil {
			return nil, sum, fmt.Errorf("id file %s line %d: %w", path, i+2, err)
		}
		ids = append(ids, id)
	}
	return ids, sum, nil
}

func parseID(keyType, text string) (any, error) {
	switch keyType {
	case "sequential":
		v, err := strconv.ParseInt(text, 10, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "uuidv1", "uuidv4", "uuidv7":
		u, err := gocql.ParseUUID(text)
		if err != nil {
			return nil, err
		}
		return u, nil
	case "ulid", "ulid_monotonic":
		b, err := hex.DecodeString(text)
		if err != nil {
			return nil, err
		}
		return b, nil
	default:
		return nil, fmt.Errorf("parseID: unknown key type %q", keyType)
	}
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
