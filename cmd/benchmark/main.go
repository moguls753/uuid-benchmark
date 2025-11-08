package main

import (
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

func main() {
	// Command-line flags
	operation := flag.String("operation", "insert", "Operation to benchmark (insert, read-point, read-range, read-scan, update-point, update-batch)")
	keyType := flag.String("key-type", "bigserial", "Key type to benchmark (bigserial, uuidv4)")
	numRecords := flag.Int("num-records", 10000, "Number of records to insert (for insert operation)")
	numOps := flag.Int("num-ops", 1000, "Number of operations to perform (for read/update operations)")
	connections := flag.Int("connections", 1, "Number of concurrent database connections (1 = sequential)")
	batchSize := flag.Int("batch-size", 1, "Batch size for inserts/updates (1 = row-by-row)")
	rangeSize := flag.Int("range-size", 100, "Range size for range scans")
	flag.Parse()

	// Start fresh PostgreSQL container
	startContainer()
	defer stopContainer()

	fmt.Println("UUID Benchmark - PostgreSQL")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Operation:    %s\n", *operation)
	fmt.Printf("Key Type:     %s\n", *keyType)
	fmt.Printf("Connections:  %d\n", *connections)
	if *operation == "insert" {
		fmt.Printf("Records:      %d\n", *numRecords)
		fmt.Printf("Batch Size:   %d\n", *batchSize)
	} else if *operation == "read-range" {
		fmt.Printf("Operations:   %d\n", *numOps)
		fmt.Printf("Range Size:   %d\n", *rangeSize)
	} else if strings.HasPrefix(*operation, "read") || strings.HasPrefix(*operation, "update") {
		fmt.Printf("Operations:   %d\n", *numOps)
		if strings.Contains(*operation, "batch") {
			fmt.Printf("Batch Size:   %d\n", *batchSize)
		}
	}
	fmt.Println()

	// Validate key type
	if *keyType != "bigserial" && *keyType != "uuidv4" {
		log.Fatalf("Invalid key-type: %s (must be 'bigserial' or 'uuidv4')", *keyType)
	}

	// Create PostgreSQL benchmarker
	bench := postgres.New()

	// Connect to database
	err := bench.Connect()
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer bench.Close()
	fmt.Println("✓ Connected to PostgreSQL")

	// Execute operation
	switch *operation {
	case "insert":
		runInsertBenchmark(bench, *keyType, *numRecords, *connections, *batchSize)

	case "read-point":
		runReadPointBenchmark(bench, *keyType, *numOps, *connections, *numRecords)

	case "read-range":
		runReadRangeBenchmark(bench, *keyType, *numOps, *rangeSize, *numRecords)

	case "read-scan":
		runReadScanBenchmark(bench, *keyType)

	case "update-point":
		runUpdatePointBenchmark(bench, *keyType, *numOps, *connections, *numRecords)

	case "update-batch":
		runUpdateBatchBenchmark(bench, *keyType, *numOps, *batchSize, *numRecords)

	default:
		log.Fatalf("Invalid operation: %s", *operation)
	}

	fmt.Println()
	fmt.Println("Benchmark completed successfully!")
}

func runInsertBenchmark(bench *postgres.PostgresBenchmarker, keyType string, numRecords, connections, batchSize int) {
	// Create table
	err := bench.CreateTable(keyType)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("✓ Created table: bench_%s\n", keyType)

	// Insert records
	if connections == 1 {
		// Sequential insert
		if batchSize == 1 {
			fmt.Printf("→ Inserting %d records (sequential, row-by-row)...\n", numRecords)
		} else {
			fmt.Printf("→ Inserting %d records (sequential, batch size %d)...\n", numRecords, batchSize)
		}
		duration, err := bench.InsertRecords(keyType, numRecords, batchSize)
		if err != nil {
			log.Fatalf("Failed to insert records: %v", err)
		}

		throughput := float64(numRecords) / duration.Seconds()
		fmt.Printf("✓ Inserted %d records in %s\n", numRecords, duration.Round(time.Millisecond))
		fmt.Printf("✓ Throughput: %.2f records/sec\n", throughput)
	} else {
		// Concurrent insert
		if batchSize == 1 {
			fmt.Printf("→ Inserting %d records with %d concurrent connections (row-by-row)...\n", numRecords, connections)
		} else {
			fmt.Printf("→ Inserting %d records with %d concurrent connections (batch size %d)...\n", numRecords, connections, batchSize)
		}
		results, err := bench.InsertRecordsConcurrent(keyType, numRecords, connections, batchSize)
		if err != nil {
			log.Fatalf("Failed to insert records: %v", err)
		}

		fmt.Printf("✓ Inserted %d records in %s (%d connections)\n", numRecords, results.Duration.Round(time.Millisecond), connections)
		fmt.Printf("✓ Throughput: %.2f records/sec\n", results.Throughput)
		if batchSize == 1 {
			fmt.Printf("✓ Latency p50: %v (per operation)\n", results.LatencyP50.Round(time.Microsecond))
			fmt.Printf("✓ Latency p95: %v (per operation)\n", results.LatencyP95.Round(time.Microsecond))
			fmt.Printf("✓ Latency p99: %v (per operation)\n", results.LatencyP99.Round(time.Microsecond))
		} else {
			fmt.Printf("✓ Latency p50: %v (per batch of %d)\n", results.LatencyP50.Round(time.Microsecond), batchSize)
			fmt.Printf("✓ Latency p95: %v (per batch of %d)\n", results.LatencyP95.Round(time.Microsecond), batchSize)
			fmt.Printf("✓ Latency p99: %v (per batch of %d)\n", results.LatencyP99.Round(time.Microsecond), batchSize)
		}
		if results.ErrorCount > 0 {
			fmt.Printf("⚠ Errors: %d / %d\n", results.ErrorCount, results.TotalOps)
		}
	}

	// Measure metrics
	results, err := bench.MeasureMetrics()
	if err != nil {
		log.Fatalf("Failed to measure metrics: %v", err)
	}

	// Display results
	fmt.Printf("✓ Page splits: %d\n", results.PageSplits)
	fmt.Printf("✓ Table size: %s\n", benchmark.FormatBytes(results.TableSize))
	fmt.Printf("✓ Index size: %s\n", benchmark.FormatBytes(results.IndexSize))
	fmt.Printf("✓ Total size: %s\n", benchmark.FormatBytes(results.TableSize+results.IndexSize))

	fmt.Println()
	fmt.Println("Index Statistics:")
	fmt.Printf("  Fragmentation:    %.2f%%\n", results.Fragmentation.FragmentationPercent)
	fmt.Printf("  Avg Leaf Density: %.2f%%\n", results.Fragmentation.AvgLeafDensity)
	fmt.Printf("  Leaf Pages:       %d\n", results.Fragmentation.LeafPages)
	fmt.Printf("  Empty Pages:      %d\n", results.Fragmentation.EmptyPages)
}

func runReadPointBenchmark(bench *postgres.PostgresBenchmarker, keyType string, numOps, connections, numTotalRecords int) {
	fmt.Printf("→ Performing %d point lookups", numOps)
	if connections > 1 {
		fmt.Printf(" with %d concurrent connections", connections)
	}
	fmt.Println("...")

	if connections == 1 {
		results, err := bench.ReadRandomRecords(keyType, numOps, numTotalRecords)
		if err != nil {
			log.Fatalf("Failed to read records: %v", err)
		}

		fmt.Printf("✓ Completed %d reads in %s\n", results.TotalReads, results.Duration.Round(time.Millisecond))
		fmt.Printf("✓ Throughput: %.2f reads/sec\n", results.Throughput)
		fmt.Printf("✓ Latency p50: %v\n", results.LatencyP50.Round(time.Microsecond))
		fmt.Printf("✓ Latency p95: %v\n", results.LatencyP95.Round(time.Microsecond))
		fmt.Printf("✓ Latency p99: %v\n", results.LatencyP99.Round(time.Microsecond))
		fmt.Printf("✓ Rows returned: %d\n", results.RowsReturned)
	} else {
		results, err := bench.ReadRandomRecordsConcurrent(keyType, numOps, connections, numTotalRecords)
		if err != nil {
			log.Fatalf("Failed to read records: %v", err)
		}

		fmt.Printf("✓ Completed %d reads in %s (%d connections)\n", results.TotalOps, results.Duration.Round(time.Millisecond), connections)
		fmt.Printf("✓ Throughput: %.2f reads/sec\n", results.Throughput)
		fmt.Printf("✓ Latency p50: %v\n", results.LatencyP50.Round(time.Microsecond))
		fmt.Printf("✓ Latency p95: %v\n", results.LatencyP95.Round(time.Microsecond))
		fmt.Printf("✓ Latency p99: %v\n", results.LatencyP99.Round(time.Microsecond))
		if results.ErrorCount > 0 {
			fmt.Printf("⚠ Errors: %d / %d\n", results.ErrorCount, results.TotalOps)
		}
	}
}

func runReadRangeBenchmark(bench *postgres.PostgresBenchmarker, keyType string, numOps, rangeSize, numTotalRecords int) {
	fmt.Printf("→ Performing %d range scans (range size: %d)...\n", numOps, rangeSize)

	results, err := bench.ReadRangeScans(keyType, numOps, rangeSize, numTotalRecords)
	if err != nil {
		log.Fatalf("Failed to perform range scans: %v", err)
	}

	fmt.Printf("✓ Completed %d scans in %s\n", results.TotalReads, results.Duration.Round(time.Millisecond))
	fmt.Printf("✓ Throughput: %.2f scans/sec\n", results.Throughput)
	fmt.Printf("✓ Latency p50: %v\n", results.LatencyP50.Round(time.Microsecond))
	fmt.Printf("✓ Latency p95: %v\n", results.LatencyP95.Round(time.Microsecond))
	fmt.Printf("✓ Latency p99: %v\n", results.LatencyP99.Round(time.Microsecond))
	fmt.Printf("✓ Total rows returned: %d (avg: %.1f per scan)\n", results.RowsReturned, float64(results.RowsReturned)/float64(results.TotalReads))
}

func runReadScanBenchmark(bench *postgres.PostgresBenchmarker, keyType string) {
	fmt.Println("→ Performing full table scan...")

	duration, rowCount, err := bench.ReadSequentialScan(keyType)
	if err != nil {
		log.Fatalf("Failed to perform sequential scan: %v", err)
	}

	throughput := float64(rowCount) / duration.Seconds()
	fmt.Printf("✓ Scanned %d rows in %s\n", rowCount, duration.Round(time.Millisecond))
	fmt.Printf("✓ Throughput: %.2f rows/sec\n", throughput)
}

func runUpdatePointBenchmark(bench *postgres.PostgresBenchmarker, keyType string, numOps, connections, numTotalRecords int) {
	fmt.Printf("→ Performing %d point updates", numOps)
	if connections > 1 {
		fmt.Printf(" with %d concurrent connections", connections)
	}
	fmt.Println("...")

	if connections == 1 {
		results, err := bench.UpdateRandomRecords(keyType, numOps, numTotalRecords)
		if err != nil {
			log.Fatalf("Failed to update records: %v", err)
		}

		fmt.Printf("✓ Completed %d updates in %s\n", results.TotalUpdates, results.Duration.Round(time.Millisecond))
		fmt.Printf("✓ Throughput: %.2f updates/sec\n", results.Throughput)
		fmt.Printf("✓ Latency p50: %v\n", results.LatencyP50.Round(time.Microsecond))
		fmt.Printf("✓ Latency p95: %v\n", results.LatencyP95.Round(time.Microsecond))
		fmt.Printf("✓ Latency p99: %v\n", results.LatencyP99.Round(time.Microsecond))
		if results.ErrorCount > 0 {
			fmt.Printf("⚠ Errors: %d / %d\n", results.ErrorCount, results.TotalUpdates)
		}
	} else {
		results, err := bench.UpdateRandomRecordsConcurrent(keyType, numOps, connections, numTotalRecords)
		if err != nil {
			log.Fatalf("Failed to update records: %v", err)
		}

		fmt.Printf("✓ Completed %d updates in %s (%d connections)\n", results.TotalOps, results.Duration.Round(time.Millisecond), connections)
		fmt.Printf("✓ Throughput: %.2f updates/sec\n", results.Throughput)
		fmt.Printf("✓ Latency p50: %v\n", results.LatencyP50.Round(time.Microsecond))
		fmt.Printf("✓ Latency p95: %v\n", results.LatencyP95.Round(time.Microsecond))
		fmt.Printf("✓ Latency p99: %v\n", results.LatencyP99.Round(time.Microsecond))
		if results.ErrorCount > 0 {
			fmt.Printf("⚠ Errors: %d / %d\n", results.ErrorCount, results.TotalOps)
		}
	}
}

func runUpdateBatchBenchmark(bench *postgres.PostgresBenchmarker, keyType string, numOps, batchSize, numTotalRecords int) {
	fmt.Printf("→ Performing %d updates (batch size: %d)...\n", numOps, batchSize)

	results, err := bench.UpdateBatchRecords(keyType, numOps, batchSize, numTotalRecords)
	if err != nil {
		log.Fatalf("Failed to batch update records: %v", err)
	}

	fmt.Printf("✓ Completed %d updates in %s\n", results.TotalUpdates, results.Duration.Round(time.Millisecond))
	fmt.Printf("✓ Throughput: %.2f updates/sec\n", results.Throughput)
	fmt.Printf("✓ Latency p50: %v (per batch of %d)\n", results.LatencyP50.Round(time.Microsecond), batchSize)
	fmt.Printf("✓ Latency p95: %v (per batch of %d)\n", results.LatencyP95.Round(time.Microsecond), batchSize)
	fmt.Printf("✓ Latency p99: %v (per batch of %d)\n", results.LatencyP99.Round(time.Microsecond), batchSize)
	if results.ErrorCount > 0 {
		fmt.Printf("⚠ Errors: %d / %d\n", results.ErrorCount, results.TotalUpdates)
	}
}

func startContainer() {
	fmt.Println("🐳 Starting fresh PostgreSQL container...")

	cmd := exec.Command("docker", "compose",
		"-f", "docker/docker-compose.postgres.yml",
		"up", "-d")

	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to start container: %v\nOutput: %s", err, string(output))
	}

	// Wait for PostgreSQL to be ready
	fmt.Println("⏳ Waiting for PostgreSQL to initialize...")
	time.Sleep(5 * time.Second)

	fmt.Println("✅ Container ready\n")
}

func stopContainer() {
	fmt.Println("\n🧹 Cleaning up container...")

	cmd := exec.Command("docker", "compose",
		"-f", "docker/docker-compose.postgres.yml",
		"down", "-v")

	// Ignore errors on cleanup - container might already be stopped
	cmd.Run()

	fmt.Println("✅ Container stopped and removed")
}
