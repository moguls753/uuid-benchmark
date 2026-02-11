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
	output := flag.String("output", "", "Output CSV file for statistical results (only in multi-run mode)")
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
		runReadPerformance(*numRecords, *numOps, *numRuns, currentDB.id)

	case "update-performance":
		runUpdatePerformance(*numRecords, *numOps, *batchSize, *numRuns, currentDB.id)

	case "mixed-insert-heavy":
		runMixedWorkloadInsertHeavy(*numOps, *connections, *batchSize, *numRuns, currentDB.id)

	case "mixed-read-update":
		runMixedWorkloadReadUpdate(*numOps, *connections, *numRuns, currentDB.id)

	case "all":
		runAllScenarios(*numRecords, *numOps, *connections, *batchSize, *numRuns, *output, currentDB.id)

	default:
		log.Fatalf("Invalid scenario: %s", *scenario)
	}

	fmt.Println()
	fmt.Println("All scenarios completed successfully!")
}

func runInsertPerformance(numRecords, batchSize, connections, numRuns int, outputFile, database string) {
	if numRuns == 1 {
		results := make(map[string]*benchmark.InsertPerformanceResult)

		for _, keyType := range allKeyTypes {
			fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
			fmt.Println(strings.Repeat("-", 70))

			container.Start(currentDB.containerCfg)

			result, err := currentDB.insertFunc(keyType, numRecords, batchSize, connections)
			if err != nil {
				container.Stop(currentDB.containerCfg.ComposeFile)
				log.Fatalf("Scenario failed for %s: %v", keyType, err)
			}

			results[keyType] = result
			container.Stop(currentDB.containerCfg.ComposeFile)
		}

		display.InsertPerformance(results, allKeyTypes, connections, batchSize, database)
	} else {
		statsResults := make(map[string]map[string]statistics.Stats)

		for _, keyType := range allKeyTypes {
			fmt.Printf("\nTesting %s (%d runs)\n", strings.ToUpper(keyType), numRuns)
			fmt.Println(strings.Repeat("-", 70))

			runs := make([]*benchmark.InsertPerformanceResult, numRuns)

			for i := 0; i < numRuns; i++ {
				fmt.Printf("  Run %d/%d... ", i+1, numRuns)

				container.Start(currentDB.containerCfg)

				result, err := currentDB.insertFunc(keyType, numRecords, batchSize, connections)
				if err != nil {
					container.Stop(currentDB.containerCfg.ComposeFile)
					log.Fatalf("Run %d failed for %s: %v", i+1, keyType, err)
				}

				runs[i] = result
				container.Stop(currentDB.containerCfg.ComposeFile)

				fmt.Println("done")
			}

			statsResults[keyType] = aggregateInsertPerformanceResults(runs)
		}

		display.InsertPerformanceStatistics(statsResults, allKeyTypes, numRecords, connections, batchSize, numRuns, database)

		if outputFile != "" {
			fmt.Printf("\nExporting results to CSV...\n")

			if err := export.InsertPerformanceStatsToCSV(statsResults, allKeyTypes, outputFile); err != nil {
				log.Printf("Warning: Failed to export stats CSV: %v", err)
			} else {
				fmt.Printf("✓ Statistical summary: %s\n", outputFile)
			}

			rawFile := strings.Replace(outputFile, ".csv", "_raw.csv", 1)
			if rawFile == outputFile {
				rawFile = outputFile + ".raw"
			}
			if err := export.InsertPerformanceRawRunsToCSV(statsResults, allKeyTypes, rawFile); err != nil {
				log.Printf("Warning: Failed to export raw runs CSV: %v", err)
			} else {
				fmt.Printf("✓ Raw runs data: %s\n", rawFile)
			}
		}
	}
}

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

	return map[string]statistics.Stats{
		"throughput":          statistics.Calculate(throughput),
		"page_splits":         statistics.Calculate(pageSplits),
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
}

func runReadPerformance(numRecords, numOps, numRuns int, database string) {
	results := make(map[string]*benchmark.ReadPerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.readFunc(keyType, numRecords, numOps)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	display.ReadPerformance(results, allKeyTypes, database)
}

func runUpdatePerformance(numRecords, numOps, batchSize, numRuns int, database string) {
	results := make(map[string]*benchmark.UpdatePerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.updateFunc(keyType, numRecords, numOps, batchSize)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	display.UpdatePerformance(results, allKeyTypes, database)
}

func runMixedWorkloadInsertHeavy(totalOps, connections, batchSize, numRuns int, database string) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.mixedInsertHeavy(keyType, totalOps, connections, batchSize)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	display.MixedWorkload(results, allKeyTypes, "Insert-Heavy (70% insert, 30% read)", database)
}

func runMixedWorkloadReadUpdate(totalOps, connections, numRuns int, database string) {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.mixedReadUpdate(keyType, totalOps, connections)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	display.MixedWorkload(results, allKeyTypes, "YCSB-A (50% read, 50% update)", database)
}

// Helper functions for runAllScenarios - collect results without displaying
func collectInsertPerformanceResults(numRecords, batchSize, connections int) map[string]*benchmark.InsertPerformanceResult {
	results := make(map[string]*benchmark.InsertPerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.insertFunc(keyType, numRecords, batchSize, connections)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	return results
}

func collectReadPerformanceResults(numRecords, numOps int) map[string]*benchmark.ReadPerformanceResult {
	results := make(map[string]*benchmark.ReadPerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.readFunc(keyType, numRecords, numOps)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	return results
}

func collectUpdatePerformanceResults(numRecords, numOps, batchSize int) map[string]*benchmark.UpdatePerformanceResult {
	results := make(map[string]*benchmark.UpdatePerformanceResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.updateFunc(keyType, numRecords, numOps, batchSize)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	return results
}

func collectMixedWorkloadInsertHeavyResults(totalOps, connections, batchSize int) map[string]*benchmark.MixedWorkloadResult {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.mixedInsertHeavy(keyType, totalOps, connections, batchSize)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	return results
}

func collectMixedWorkloadReadUpdateResults(totalOps, connections int) map[string]*benchmark.MixedWorkloadResult {
	results := make(map[string]*benchmark.MixedWorkloadResult)

	for _, keyType := range allKeyTypes {
		fmt.Printf("\nTesting %s\n", strings.ToUpper(keyType))
		fmt.Println(strings.Repeat("-", 70))

		container.Start(currentDB.containerCfg)

		result, err := currentDB.mixedReadUpdate(keyType, totalOps, connections)
		if err != nil {
			container.Stop(currentDB.containerCfg.ComposeFile)
			log.Fatalf("Scenario failed for %s: %v", keyType, err)
		}

		results[keyType] = result
		container.Stop(currentDB.containerCfg.ComposeFile)
	}

	return results
}

func buildWorkloadBinary() {
	fmt.Println("Building workload binary for NoSQL databases...")
	path, err := workload.BuildBinary()
	if err != nil {
		log.Fatalf("Failed to build workload binary: %v", err)
	}
	fmt.Printf("Workload binary built: %s\n", path)
}

func runAllScenarios(numRecords, numOps, connections, batchSize, numRuns int, output, database string) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("RUNNING ALL SCENARIOS - COMPREHENSIVE BENCHMARK SUITE")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println()

	startTime := time.Now()

	// Collect all results first
	fmt.Println("\n[1/5] INSERT PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	insertResults := collectInsertPerformanceResults(numRecords, batchSize, connections)

	fmt.Println("\n[2/5] READ PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	readResults := collectReadPerformanceResults(numRecords, numOps)

	fmt.Println("\n[3/5] UPDATE PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	updateResults := collectUpdatePerformanceResults(numRecords, numOps, batchSize)

	fmt.Println("\n[4/5] MIXED INSERT-HEAVY")
	fmt.Println(strings.Repeat("=", 100))
	mixedInsertHeavyResults := collectMixedWorkloadInsertHeavyResults(numOps, connections, batchSize)

	fmt.Println("\n[5/5] MIXED READ-UPDATE (YCSB-A)")
	fmt.Println(strings.Repeat("=", 100))
	mixedReadUpdateResults := collectMixedWorkloadReadUpdateResults(numOps, connections)

	totalDuration := time.Since(startTime)
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("ALL SCENARIOS COMPLETED IN %s\n", totalDuration.Round(time.Second))
	fmt.Println(strings.Repeat("=", 100))

	// Display all tables
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("BENCHMARK RESULTS SUMMARY")
	fmt.Println(strings.Repeat("=", 100))

	display.InsertPerformance(insertResults, allKeyTypes, connections, batchSize, database)
	display.ReadPerformance(readResults, allKeyTypes, database)
	display.UpdatePerformance(updateResults, allKeyTypes, database)
	display.MixedWorkload(mixedInsertHeavyResults, allKeyTypes, "Insert-Heavy (70% insert, 30% read)", database)
	display.MixedWorkload(mixedReadUpdateResults, allKeyTypes, "YCSB-A (50% read, 50% update)", database)
}
