package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

func main() {
	// Command-line flags
	keyType := flag.String("key-type", "bigserial", "Key type to benchmark (bigserial, uuidv4)")
	numRecords := flag.Int("num-records", 10000, "Number of records to insert")
	flag.Parse()

	fmt.Println("UUID Benchmark - PostgreSQL")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Key Type: %s\n", *keyType)
	fmt.Printf("Records:  %d\n", *numRecords)
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

	// Create table
	err = bench.CreateTable(*keyType)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("✓ Created table: bench_%s\n", *keyType)

	// Insert records and measure time
	fmt.Printf("→ Inserting %d records...\n", *numRecords)
	duration, err := bench.InsertRecords(*keyType, *numRecords)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}

	throughput := float64(*numRecords) / duration.Seconds()
	fmt.Printf("✓ Inserted %d records in %s\n", *numRecords, duration.Round(time.Millisecond))
	fmt.Printf("✓ Throughput: %.2f records/sec\n", throughput)

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

	fmt.Println()
	fmt.Println("Benchmark completed successfully!")
}
