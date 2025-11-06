package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/lib/pq"
)

func main() {
	fmt.Println("UUID Benchmark - Step 1: Database Connection")
	fmt.Println("=" * 50)
	fmt.Println()

	// Connection string for PostgreSQL
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=uuid_benchmark sslmode=disable"

	// Open connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}

	fmt.Println("✓ Successfully connected to PostgreSQL!")

	// Get PostgreSQL version
	var version string
	err = db.QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		log.Fatalf("Failed to query version: %v", err)
	}

	fmt.Printf("✓ PostgreSQL version: %s\n", version)
}
