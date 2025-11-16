package postgres

import (
	"database/sql"
	"fmt"
	"time"
)

func (p *PostgresBenchmarker) Connect() error {
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=uuid_benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	// Test connection
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	p.db = db

	// Enable pgstattuple extension for index statistics
	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pgstattuple")
	if err != nil {
		return fmt.Errorf("enable pgstattuple extension: %w", err)
	}

	// Enable pg_walinspect extension for WAL analysis (PostgreSQL 15+), page splits counts
	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pg_walinspect")
	if err != nil {
		return fmt.Errorf("enable pg_walinspect extension: %w", err)
	}

	return nil
}

// CreateTable creates the benchmark table with the specified key type
func (p *PostgresBenchmarker) CreateTable(keyType string) error {
	p.keyType = keyType
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	// Drop table if exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", p.tableName)
	_, err := p.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	// Create table based on key type
	var createSQL string
	switch keyType {
	case "bigserial":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGSERIAL PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	case "uuidv4":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id UUID PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	case "uuidv7":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id UUID PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	case "ulid":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id TEXT PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	case "uuidv1":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id UUID PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	_, err = p.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// Close closes the database connection
func (p *PostgresBenchmarker) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// WaitForReady waits for PostgreSQL to be ready with retry logic
// This is used during container startup to ensure the database is accepting connections
func WaitForReady() error {
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=uuid_benchmark sslmode=disable"
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		db, err := sql.Open("postgres", connStr)
		if err == nil {
			if err := db.Ping(); err == nil {
				db.Close()
				return nil // Success!
			}
			db.Close()
		}
		time.Sleep(500 * time.Millisecond) // Retry every 500ms
	}

	return fmt.Errorf("timeout waiting for PostgreSQL after %v", timeout)
}
