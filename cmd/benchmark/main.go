package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
	"github.com/moguls753/uuid-benchmark/internal/container"
	"github.com/moguls753/uuid-benchmark/internal/display"
	"github.com/moguls753/uuid-benchmark/internal/export"
	"github.com/moguls753/uuid-benchmark/internal/runner"
)

var allKeyTypes = []string{"sequential", "uuidv4", "uuidv7", "ulid", "ulid_monotonic", "uuidv1"}

// Database configuration
type dbConfig struct {
	name             string
	id               string // canonical lowercase name for display logic ("postgres", "mysql", "mongodb", "cassandra")
	start            func() error
	stop             func() error
	insertFunc       func(string, int, int, int) (*benchmark.InsertPerformanceResult, error)
	readFunc         func(string, int, int) (*benchmark.ReadPerformanceResult, error)
	updateFunc       func(string, int, int, int) (*benchmark.UpdatePerformanceResult, error)
	mixedInsertHeavy func(string, int, int, int) (*benchmark.MixedWorkloadResult, error)
	mixedReadUpdate  func(string, int, int) (*benchmark.MixedWorkloadResult, error)
}

var postgresDB = dbConfig{
	name:             "PostgreSQL",
	id:               "postgres",
	start:            func() error { container.Start(container.PostgresConfig); return nil },
	stop:             func() error { container.Stop(container.PostgresConfig.ComposeFile); return nil },
	insertFunc:       runner.InsertPerformance,
	readFunc:         runner.ReadPerformance,
	updateFunc:       runner.UpdatePerformance,
	mixedInsertHeavy: runner.MixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MixedWorkloadReadUpdate,
}

var mysqlDB = dbConfig{
	name:             "MySQL",
	id:               "mysql",
	start:            func() error { container.Start(container.MySQLConfig); return nil },
	stop:             func() error { container.Stop(container.MySQLConfig.ComposeFile); return nil },
	insertFunc:       runner.MySQLInsertPerformance,
	readFunc:         runner.MySQLReadPerformance,
	updateFunc:       runner.MySQLUpdatePerformance,
	mixedInsertHeavy: runner.MySQLMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MySQLMixedWorkloadReadUpdate,
}

var mongodbDB = dbConfig{
	name:             "MongoDB",
	id:               "mongodb",
	start:            func() error { container.Start(container.MongoDBConfig); return nil },
	stop:             func() error { container.Stop(container.MongoDBConfig.ComposeFile); return nil },
	insertFunc:       runner.MongoDBInsertPerformance,
	readFunc:         runner.MongoDBReadPerformance,
	updateFunc:       runner.MongoDBUpdatePerformance,
	mixedInsertHeavy: runner.MongoDBMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MongoDBMixedWorkloadReadUpdate,
}

// cassandraDBConfig builds a Cassandra dbConfig that closes over the cluster
// config and Backend, since runner.Cassandra* signatures take both but the
// shared dbConfig function types do not (the other databases don't use them).
func cassandraDBConfig(cfg cluster.ClusterConfig, backend cluster.Backend) dbConfig {
	return dbConfig{
		name: "Cassandra",
		id:   "cassandra",
		start: func() error {
			if err := backend.Start(); err != nil {
				return fmt.Errorf("backend start: %w", err)
			}
			if err := backend.WaitForReady(); err != nil {
				return fmt.Errorf("backend wait for ready: %w", err)
			}
			return nil
		},
		stop: func() error {
			err := backend.Stop()
			// The container is gone; the cached "we already docker cp'd the
			// binary into <name>" entry would otherwise short-circuit the next
			// copy and leave the fresh container with no /tmp/workload.
			workload.ResetCopyCache()
			if err != nil {
				return fmt.Errorf("backend stop: %w", err)
			}
			return nil
		},
		insertFunc: func(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
			return runner.CassandraInsertPerformance(keyType, numRecords, batchSize, connections, cfg, backend)
		},
		readFunc: func(keyType string, numRecords, numReads int) (*benchmark.ReadPerformanceResult, error) {
			return runner.CassandraReadPerformance(keyType, numRecords, numReads, cfg, backend)
		},
		updateFunc: func(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
			return runner.CassandraUpdatePerformance(keyType, numRecords, numUpdates, batchSize, cfg, backend)
		},
		mixedInsertHeavy: func(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
			return runner.CassandraMixedWorkloadInsertHeavy(keyType, totalOps, connections, batchSize, cfg, backend)
		},
		mixedReadUpdate: func(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
			return runner.CassandraMixedWorkloadReadUpdate(keyType, totalOps, connections, cfg, backend)
		},
	}
}

var currentDB dbConfig

func main() {
	database := flag.String("database", "postgres", "Database to benchmark (postgres, mysql, mongodb, cassandra)")
	scenario := flag.String("scenario", "insert-performance", "Scenario to run (insert-performance, read-performance, update-performance, mixed-insert-heavy, mixed-read-update, all)")
	numRecords := flag.Int("num-records", 100000, "Number of records for insert operations")
	numOps := flag.Int("num-ops", 10000, "Number of operations for read/update/mixed scenarios")
	connections := flag.Int("connections", 1, "Number of concurrent connections")
	batchSize := flag.Int("batch-size", 100, "Batch size for inserts/updates")
	numRuns := flag.Int("num-runs", 1, "Number of runs per UUID type (for statistical analysis)")
	numBuckets := flag.Int("num-buckets", 1000, "Number of Cassandra partition buckets")
	// Cassandra cluster topology flags — only consulted for -database=cassandra.
	clusterMode := flag.String("cluster-mode", "local-single", "Cassandra cluster mode: local-single, local-cluster, remote-cluster (Cassandra only)")
	nodes := flag.String("nodes", "", "Comma-separated hostnames for remote-cluster mode (e.g. taurus5,taurus6,taurus7)")
	sshUser := flag.String("ssh-user", "", "SSH user for remote-cluster mode")
	sshKey := flag.String("ssh-key", "", "SSH private key path for remote-cluster mode (default: ssh-agent / ~/.ssh/id_*)")
	replicationFactor := flag.Int("replication-factor", 0, "Cassandra replication factor (default: 1 for local-single, 3 for cluster modes)")
	consistency := flag.String("consistency", "", "CQL consistency level: one, local_one, local_quorum, quorum (default: local_one for local-single, local_quorum for cluster modes)")
	clusterNodeCount := flag.Int("cluster-nodes", 3, "Number of nodes for local-cluster mode (must match docker/docker-compose.cassandra-cluster.yml service count)")
	output := flag.String("output", "", "Output CSV file for statistical results")
	flag.Parse()

	// Reject cluster-mode flags on non-Cassandra dispatch. Without this an
	// operator who copies a `-cluster-mode=remote-cluster -nodes=...`
	// invocation and just flips `-database=postgres` gets a silent
	// single-container postgres run on the orchestrator and wastes hours
	// realizing the cluster flags were ignored.
	if !isCassandra(*database) {
		var offending []string
		if *clusterMode != "local-single" {
			offending = append(offending, "-cluster-mode")
		}
		if *nodes != "" {
			offending = append(offending, "-nodes")
		}
		if *sshUser != "" {
			offending = append(offending, "-ssh-user")
		}
		if *sshKey != "" {
			offending = append(offending, "-ssh-key")
		}
		if *replicationFactor != 0 {
			offending = append(offending, "-replication-factor")
		}
		if *consistency != "" {
			offending = append(offending, "-consistency")
		}
		if *clusterNodeCount != 3 {
			offending = append(offending, "-cluster-nodes")
		}
		// -num-buckets is Cassandra-specific (only the Cassandra schema uses
		// the bucket partition key); reject when non-default for non-Cassandra.
		if *numBuckets != 1000 {
			offending = append(offending, "-num-buckets")
		}
		if len(offending) > 0 {
			log.Fatalf("cluster flags (%s) are only valid with -database=cassandra; got -database=%s",
				strings.Join(offending, ", "), *database)
		}
	}

	// Select database configuration
	switch strings.ToLower(*database) {
	case "postgres", "postgresql", "pg":
		currentDB = postgresDB
	case "mysql", "my":
		currentDB = mysqlDB
	case "mongodb", "mongo":
		currentDB = mongodbDB
		allKeyTypes = append(allKeyTypes, "objectid")
		buildWorkloadBinary()
	case "cassandra", "cass":
		if *clusterMode == "local-cluster" && *clusterNodeCount <= 0 {
			log.Fatalf("-cluster-nodes must be >= 1 (got %d)", *clusterNodeCount)
		}
		cfg, err := buildClusterConfig(*clusterMode, *nodes, *sshUser, *sshKey, *consistency, *replicationFactor, *numBuckets)
		if err != nil {
			log.Fatalf("Build cluster config: %v", err)
		}
		backend, err := buildBackend(cfg, *clusterNodeCount)
		if err != nil {
			log.Fatalf("Build backend: %v", err)
		}
		currentDB = cassandraDBConfig(cfg, backend)
		buildWorkloadBinary()
	default:
		log.Fatalf("Invalid database: %s (use 'postgres', 'mysql', 'mongodb', or 'cassandra')", *database)
	}

	fmt.Printf("UUID Benchmark - %s\n", currentDB.name)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Database:     %s\n", currentDB.name)
	if currentDB.id == "cassandra" {
		fmt.Printf("Cluster mode: %s\n", *clusterMode)
	}
	fmt.Printf("Scenario:     %s\n", *scenario)
	fmt.Printf("Records:      %d\n", *numRecords)
	if *connections > 1 {
		fmt.Printf("Connections:  %d\n", *connections)
	}
	if *batchSize > 1 {
		fmt.Printf("Batch Size:   %d\n", *batchSize)
	}
	if *numRuns > 1 {
		fmt.Printf("Runs:         %d (statistical mode)\n", *numRuns)
	}
	fmt.Printf("Testing:      %v\n", allKeyTypes)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println()

	switch *scenario {
	case "insert-performance":
		runInsertPerformance(*numRecords, *batchSize, *connections, *numRuns, *output, currentDB.id)

	case "read-performance":
		runReadPerformance(*numRecords, *numOps, *connections, *numRuns, *output, currentDB.id)

	case "update-performance":
		runUpdatePerformance(*numRecords, *numOps, *batchSize, *connections, *numRuns, *output, currentDB.id)

	case "mixed-insert-heavy":
		runMixedWorkloadInsertHeavy(*numOps, *numRecords, *connections, *batchSize, *numRuns, *output, currentDB.id)

	case "mixed-read-update":
		runMixedWorkloadReadUpdate(*numOps, *numRecords, *connections, *numRuns, *output, currentDB.id)

	case "all":
		runAllScenarios(*numRecords, *numOps, *connections, *batchSize, *numRuns, *output, currentDB.id)

	default:
		log.Fatalf("Invalid scenario: %s", *scenario)
	}

	fmt.Println()
	fmt.Println("All scenarios completed successfully!")
}

// runScenario is the generic core loop for all benchmark scenarios.
// It iterates key types, runs the workload numRuns times with fresh containers,
// aggregates results, displays them, and optionally exports CSV.
func runScenario[R any](
	scenarioName string,
	numRuns int,
	outputFile string,
	recordCount int,
	connections int,
	runOne func(keyType string) (*R, error),
	aggregate func(runs []*R) map[string]statistics.Stats,
	displaySingle func(results map[string]*R),
	displayStats func(stats map[string]map[string]statistics.Stats),
) map[string]map[string]statistics.Stats {
	allRuns := make(map[string][]*R)

	for _, keyType := range allKeyTypes {
		if numRuns > 1 {
			fmt.Printf("\nTesting %s (%d runs)\n", strings.ToUpper(keyType), numRuns)
		} else {
			fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		}
		fmt.Println(strings.Repeat("-", 70))

		for i := 0; i < numRuns; i++ {
			if numRuns > 1 {
				fmt.Printf("  Run %d/%d... ", i+1, numRuns)
			}

			if err := currentDB.start(); err != nil {
				// Best-effort cleanup: a partially-started compose stack
				// would otherwise leak until the next run's pre-teardown,
				// and for Cassandra the workload copy cache (cleared inside
				// stop()) would point at the now-gone container.
				if stopErr := currentDB.stop(); stopErr != nil {
					fmt.Printf("Warning: stop after start failure: %v\n", stopErr)
				}
				log.Fatalf("Run %d start failed for %s: %v", i+1, keyType, err)
			}

			result, err := runOne(keyType)
			if err != nil {
				if stopErr := currentDB.stop(); stopErr != nil {
					fmt.Printf("Warning: stop after failure: %v\n", stopErr)
				}
				log.Fatalf("Run %d failed for %s: %v", i+1, keyType, err)
			}

			allRuns[keyType] = append(allRuns[keyType], result)
			if err := currentDB.stop(); err != nil {
				log.Fatalf("Run %d stop failed for %s: %v", i+1, keyType, err)
			}

			if numRuns > 1 {
				fmt.Println("done")
			}
		}
	}

	allStats := make(map[string]map[string]statistics.Stats)
	for _, keyType := range allKeyTypes {
		allStats[keyType] = aggregate(allRuns[keyType])
	}

	if numRuns == 1 {
		singleResults := make(map[string]*R)
		for k, runs := range allRuns {
			singleResults[k] = runs[0]
		}
		displaySingle(singleResults)
	} else {
		displayStats(allStats)
	}

	if outputFile != "" {
		exportCSV(scenarioName, allStats, outputFile, recordCount, connections)
	}

	return allStats
}

func runInsertPerformance(numRecords, batchSize, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"insert_performance", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.InsertPerformanceResult, error) {
			return currentDB.insertFunc(keyType, numRecords, batchSize, connections)
		},
		aggregateInsertPerformanceResults,
		func(results map[string]*benchmark.InsertPerformanceResult) {
			display.InsertPerformance(results, allKeyTypes, connections, batchSize, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.InsertPerformanceStatistics(stats, allKeyTypes, numRecords, connections, batchSize, numRuns, database)
		},
	)
}

func runReadPerformance(numRecords, numOps, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"read_performance", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.ReadPerformanceResult, error) {
			return currentDB.readFunc(keyType, numRecords, numOps)
		},
		aggregateReadPerformanceResults,
		func(results map[string]*benchmark.ReadPerformanceResult) {
			display.ReadPerformance(results, allKeyTypes, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Read Performance", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runUpdatePerformance(numRecords, numOps, batchSize, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"update_performance", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.UpdatePerformanceResult, error) {
			return currentDB.updateFunc(keyType, numRecords, numOps, batchSize)
		},
		aggregateUpdatePerformanceResults,
		func(results map[string]*benchmark.UpdatePerformanceResult) {
			display.UpdatePerformance(results, allKeyTypes, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Update Performance", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runMixedWorkloadInsertHeavy(totalOps, numRecords, connections, batchSize, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"mixed_insert_heavy", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.MixedWorkloadResult, error) {
			return currentDB.mixedInsertHeavy(keyType, totalOps, connections, batchSize)
		},
		aggregateMixedWorkloadResults,
		func(results map[string]*benchmark.MixedWorkloadResult) {
			display.MixedWorkload(results, allKeyTypes, "Insert-Heavy (70% insert, 30% read)", database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Mixed Insert-Heavy (70% insert, 30% read)", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runMixedWorkloadReadUpdate(totalOps, numRecords, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"mixed_read_update", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.MixedWorkloadResult, error) {
			return currentDB.mixedReadUpdate(keyType, totalOps, connections)
		},
		aggregateMixedWorkloadResults,
		func(results map[string]*benchmark.MixedWorkloadResult) {
			display.MixedWorkload(results, allKeyTypes, "YCSB-A (50% read, 50% update)", database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Mixed YCSB-A (50% read, 50% update)", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runAllScenarios(numRecords, numOps, connections, batchSize, numRuns int, output, database string) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("RUNNING ALL SCENARIOS - COMPREHENSIVE BENCHMARK SUITE")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println()

	startTime := time.Now()

	fmt.Println("\n[1/5] INSERT PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	insertStats := runInsertPerformance(numRecords, batchSize, connections, numRuns, "", database)

	fmt.Println("\n[2/5] READ PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	readStats := runReadPerformance(numRecords, numOps, connections, numRuns, "", database)

	fmt.Println("\n[3/5] UPDATE PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	updateStats := runUpdatePerformance(numRecords, numOps, batchSize, connections, numRuns, "", database)

	fmt.Println("\n[4/5] MIXED INSERT-HEAVY")
	fmt.Println(strings.Repeat("=", 100))
	mixedIHStats := runMixedWorkloadInsertHeavy(numOps, numRecords, connections, batchSize, numRuns, "", database)

	fmt.Println("\n[5/5] MIXED READ-UPDATE (YCSB-A)")
	fmt.Println(strings.Repeat("=", 100))
	mixedRUStats := runMixedWorkloadReadUpdate(numOps, numRecords, connections, numRuns, "", database)

	totalDuration := time.Since(startTime)
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("ALL SCENARIOS COMPLETED IN %s\n", totalDuration.Round(time.Second))
	fmt.Println(strings.Repeat("=", 100))

	if output != "" {
		scenarios := []export.ScenarioStats{
			{Name: "insert_performance", RecordCount: numRecords, Connections: connections, Results: insertStats},
			{Name: "read_performance", RecordCount: numRecords, Connections: connections, Results: readStats},
			{Name: "update_performance", RecordCount: numRecords, Connections: connections, Results: updateStats},
			{Name: "mixed_insert_heavy", RecordCount: numRecords, Connections: connections, Results: mixedIHStats},
			{Name: "mixed_read_update", RecordCount: numRecords, Connections: connections, Results: mixedRUStats},
		}

		fmt.Printf("\nExporting results to CSV...\n")

		if err := export.StatsToCSV(scenarios, allKeyTypes, output); err != nil {
			log.Printf("Warning: Failed to export stats CSV: %v", err)
		} else {
			fmt.Printf("  Statistical summary: %s\n", output)
		}

		rawFile := strings.Replace(output, ".csv", "_raw.csv", 1)
		if rawFile == output {
			rawFile = output + ".raw"
		}
		if err := export.RawRunsToCSV(scenarios, allKeyTypes, rawFile); err != nil {
			log.Printf("Warning: Failed to export raw runs CSV: %v", err)
		} else {
			fmt.Printf("  Raw runs data: %s\n", rawFile)
		}
	}
}

// Aggregation functions

func aggregateInsertPerformanceResults(runs []*benchmark.InsertPerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	throughput := make([]float64, numRuns)
	pageSplits := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	avgLeafDensity := make([]float64, numRuns)
	tableSizeMB := make([]float64, numRuns)
	indexSizeMB := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	for i, run := range runs {
		throughput[i] = run.Throughput
		pageSplits[i] = float64(run.PageSplits)
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		avgLeafDensity[i] = run.Fragmentation.AvgLeafDensity
		tableSizeMB[i] = float64(run.TableSize) / (1024 * 1024)
		indexSizeMB[i] = float64(run.IndexSize) / (1024 * 1024)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	result := map[string]statistics.Stats{
		"throughput":          statistics.Calculate(throughput),
		"fragmentation":       statistics.Calculate(fragmentation),
		"avg_leaf_density":    statistics.Calculate(avgLeafDensity),
		"table_size_mb":       statistics.Calculate(tableSizeMB),
		"index_size_mb":       statistics.Calculate(indexSizeMB),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}

	// Cassandra uses SSTable count delta; B-tree databases use page splits
	if currentDB.id == "cassandra" {
		result["sstable_count"] = statistics.Calculate(pageSplits)
	} else {
		result["page_splits"] = statistics.Calculate(pageSplits)
	}

	return result
}

func aggregateReadPerformanceResults(runs []*benchmark.ReadPerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	readThroughput := make([]float64, numRuns)
	cacheHitRatio := make([]float64, numRuns)
	indexHitRatio := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	bloomFilterFP := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	for i, run := range runs {
		readThroughput[i] = run.ReadThroughput
		cacheHitRatio[i] = run.BufferHitRatio
		indexHitRatio[i] = run.IndexBufferHitRatio
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		bloomFilterFP[i] = float64(run.BloomFilterFP)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	result := map[string]statistics.Stats{
		"read_throughput":      statistics.Calculate(readThroughput),
		"cache_hit_ratio":     statistics.Calculate(cacheHitRatio),
		"index_hit_ratio":     statistics.Calculate(indexHitRatio),
		"fragmentation":       statistics.Calculate(fragmentation),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}

	// Bloom filter FP is only meaningful for Cassandra (LSM-tree SSTable lookups)
	if currentDB.id == "cassandra" {
		result["bloom_filter_fp"] = statistics.Calculate(bloomFilterFP)
	}

	return result
}

func aggregateUpdatePerformanceResults(runs []*benchmark.UpdatePerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	updateThroughput := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	for i, run := range runs {
		updateThroughput[i] = run.UpdateThroughput
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	return map[string]statistics.Stats{
		"update_throughput":    statistics.Calculate(updateThroughput),
		"fragmentation":       statistics.Calculate(fragmentation),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}
}

func aggregateMixedWorkloadResults(runs []*benchmark.MixedWorkloadResult) map[string]statistics.Stats {
	numRuns := len(runs)

	overallThroughput := make([]float64, numRuns)
	cacheHitRatio := make([]float64, numRuns)
	indexHitRatio := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	tableSizeMB := make([]float64, numRuns)
	indexSizeMB := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	for i, run := range runs {
		overallThroughput[i] = run.OverallThroughput
		cacheHitRatio[i] = run.BufferHitRatio
		indexHitRatio[i] = run.IndexBufferHitRatio
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		tableSizeMB[i] = float64(run.TableSize) / (1024 * 1024)
		indexSizeMB[i] = float64(run.IndexSize) / (1024 * 1024)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	return map[string]statistics.Stats{
		"overall_throughput":  statistics.Calculate(overallThroughput),
		"cache_hit_ratio":    statistics.Calculate(cacheHitRatio),
		"index_hit_ratio":    statistics.Calculate(indexHitRatio),
		"fragmentation":      statistics.Calculate(fragmentation),
		"table_size_mb":       statistics.Calculate(tableSizeMB),
		"index_size_mb":       statistics.Calculate(indexSizeMB),
		"p50_latency_us":     statistics.Calculate(p50Latency),
		"p95_latency_us":     statistics.Calculate(p95Latency),
		"p99_latency_us":     statistics.Calculate(p99Latency),
		"read_iops":          statistics.Calculate(readIOPS),
		"write_iops":         statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}
}

// exportCSV exports stats for a single scenario to CSV files
func exportCSV(scenarioName string, allStats map[string]map[string]statistics.Stats, outputFile string, recordCount, connections int) {
	scenarios := []export.ScenarioStats{{Name: scenarioName, RecordCount: recordCount, Connections: connections, Results: allStats}}

	fmt.Printf("\nExporting results to CSV...\n")

	if err := export.StatsToCSV(scenarios, allKeyTypes, outputFile); err != nil {
		log.Printf("Warning: Failed to export stats CSV: %v", err)
	} else {
		fmt.Printf("  Statistical summary: %s\n", outputFile)
	}

	rawFile := strings.Replace(outputFile, ".csv", "_raw.csv", 1)
	if rawFile == outputFile {
		rawFile = outputFile + ".raw"
	}
	if err := export.RawRunsToCSV(scenarios, allKeyTypes, rawFile); err != nil {
		log.Printf("Warning: Failed to export raw runs CSV: %v", err)
	} else {
		fmt.Printf("  Raw runs data: %s\n", rawFile)
	}
}

// buildClusterConfig assembles a cluster.ClusterConfig from CLI flags,
// applies per-mode defaults for replication factor and consistency, sets
// NumBuckets, and runs cfg.Validate(). The single-helper contract means
// callers can't accidentally skip validation by reordering setup steps.
func buildClusterConfig(mode, nodesStr, sshUser, sshKey, consistency string, rf, numBuckets int) (cluster.ClusterConfig, error) {
	cfg := cluster.ClusterConfig{
		Keyspace:   "uuid_benchmark",
		Mode:       cluster.Mode(mode),
		NumBuckets: numBuckets,
	}
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		cfg.ContactPoints = []string{"127.0.0.1"}
		if rf == 0 {
			rf = 1
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalOne)
		}
	case cluster.ModeLocalCluster:
		cfg.ContactPoints = []string{"127.0.0.1"}
		if rf == 0 {
			rf = 3
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalQuorum)
		}
	case cluster.ModeRemoteCluster:
		hosts := parseHostList(nodesStr)
		if len(hosts) == 0 {
			return cfg, fmt.Errorf("remote-cluster mode requires -nodes (comma-separated hostnames)")
		}
		cfg.ContactPoints = hosts
		cfg.Hostnames = hosts
		cfg.SSHUser = sshUser
		cfg.SSHKeyPath = sshKey
		if rf == 0 {
			rf = 3
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalQuorum)
		}
	default:
		return cfg, fmt.Errorf("unknown cluster mode %q (expected local-single, local-cluster, or remote-cluster)", mode)
	}
	cfg.ReplicationFactor = rf
	cfg.Consistency = cluster.Consistency(consistency)
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// buildBackend constructs the cluster.Backend matching cfg.Mode. localNodeCount
// is only consulted in ModeLocalCluster.
func buildBackend(cfg cluster.ClusterConfig, localNodeCount int) (cluster.Backend, error) {
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		return cluster.NewLocalSingle(), nil
	case cluster.ModeLocalCluster:
		return cluster.NewLocalCluster(localNodeCount), nil
	case cluster.ModeRemoteCluster:
		return cluster.NewRemoteCluster(cfg), nil
	}
	return nil, fmt.Errorf("unknown cluster mode %q", cfg.Mode)
}

// parseHostList splits a comma-separated host list, trimming whitespace and
// dropping empty entries. Mirrors parseContactPoints in cmd/workload/main.go
// but lives locally since the workload binary's helper isn't importable.
func parseHostList(s string) []string {
	if s == "" {
		return nil
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

// isCassandra reports whether the -database flag value names the Cassandra
// runner. Mirrors the aliases accepted in the dispatch switch.
func isCassandra(database string) bool {
	switch strings.ToLower(database) {
	case "cassandra", "cass":
		return true
	}
	return false
}

func buildWorkloadBinary() {
	fmt.Println("Building workload binary for NoSQL databases...")
	path, err := workload.BuildBinary()
	if err != nil {
		log.Fatalf("Failed to build workload binary: %v", err)
	}
	fmt.Printf("Workload binary built: %s\n", path)
}
