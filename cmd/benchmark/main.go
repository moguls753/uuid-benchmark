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
	containerCfg     container.Config
	insertFunc       func(string, int, int, int) (*benchmark.InsertPerformanceResult, error)
	readFunc         func(string, int, int) (*benchmark.ReadPerformanceResult, error)
	updateFunc       func(string, int, int, int) (*benchmark.UpdatePerformanceResult, error)
	mixedInsertHeavy func(string, int, int, int) (*benchmark.MixedWorkloadResult, error)
	mixedReadUpdate  func(string, int, int) (*benchmark.MixedWorkloadResult, error)
}

var postgresDB = dbConfig{
	name:             "PostgreSQL",
	id:               "postgres",
	containerCfg:     container.PostgresConfig,
	insertFunc:       runner.InsertPerformance,
	readFunc:         runner.ReadPerformance,
	updateFunc:       runner.UpdatePerformance,
	mixedInsertHeavy: runner.MixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MixedWorkloadReadUpdate,
}

var mysqlDB = dbConfig{
	name:             "MySQL",
	id:               "mysql",
	containerCfg:     container.MySQLConfig,
	insertFunc:       runner.MySQLInsertPerformance,
	readFunc:         runner.MySQLReadPerformance,
	updateFunc:       runner.MySQLUpdatePerformance,
	mixedInsertHeavy: runner.MySQLMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MySQLMixedWorkloadReadUpdate,
}

var mongodbDB = dbConfig{
	name:             "MongoDB",
	id:               "mongodb",
	containerCfg:     container.MongoDBConfig,
	insertFunc:       runner.MongoDBInsertPerformance,
	readFunc:         runner.MongoDBReadPerformance,
	updateFunc:       runner.MongoDBUpdatePerformance,
	mixedInsertHeavy: runner.MongoDBMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MongoDBMixedWorkloadReadUpdate,
}

var cassandraDB = dbConfig{
	name:             "Cassandra",
	id:               "cassandra",
	containerCfg:     container.CassandraConfig,
	insertFunc:       runner.CassandraInsertPerformance,
	readFunc:         runner.CassandraReadPerformance,
	updateFunc:       runner.CassandraUpdatePerformance,
	mixedInsertHeavy: runner.CassandraMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.CassandraMixedWorkloadReadUpdate,
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
	output := flag.String("output", "", "Output CSV file for statistical results")
	flag.Parse()

	// Select database configuration
	switch strings.ToLower(*database) {
	case "postgres", "postgresql", "pg":
		currentDB = postgresDB
	case "mysql", "my":
		currentDB = mysqlDB
	case "mongodb", "mongo":
		currentDB = mongodbDB
		buildWorkloadBinary()
	case "cassandra", "cass":
		currentDB = cassandraDB
		buildWorkloadBinary()
	default:
		log.Fatalf("Invalid database: %s (use 'postgres', 'mysql', 'mongodb', or 'cassandra')", *database)
	}

	fmt.Printf("UUID Benchmark - %s\n", currentDB.name)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Database:     %s\n", currentDB.name)
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

			container.Start(currentDB.containerCfg)

			result, err := runOne(keyType)
			if err != nil {
				container.Stop(currentDB.containerCfg.ComposeFile)
				log.Fatalf("Run %d failed for %s: %v", i+1, keyType, err)
			}

			allRuns[keyType] = append(allRuns[keyType], result)
			container.Stop(currentDB.containerCfg.ComposeFile)

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

func buildWorkloadBinary() {
	fmt.Println("Building workload binary for NoSQL databases...")
	path, err := workload.BuildBinary()
	if err != nil {
		log.Fatalf("Failed to build workload binary: %v", err)
	}
	fmt.Printf("Workload binary built: %s\n", path)
}
