package postgres

import (
	"database/sql"
	"fmt"
	"time"
)

const (
	dbHost     = "localhost"
	dbPort     = "5432"
	dbUser     = "benchmark"
	dbPassword = "benchmark123"
	dbName     = "uuid_benchmark"
)

func (p *PostgresBenchmarker) Connect() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	p.db = db

	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pgstattuple")
	if err != nil {
		return fmt.Errorf("enable pgstattuple extension: %w", err)
	}

	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pg_walinspect")
	if err != nil {
		return fmt.Errorf("enable pg_walinspect extension: %w", err)
	}

	_, err = p.db.Exec(`CREATE EXTENSION IF NOT EXISTS "uuid-ossp"`)
	if err != nil {
		return fmt.Errorf("enable uuid-ossp extension: %w", err)
	}

	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pgx_ulid")
	if err != nil {
		return fmt.Errorf("enable pgx_ulid extension: %w", err)
	}

	return nil
}

func (p *PostgresBenchmarker) CreateTable(keyType string) error {
	p.keyType = keyType
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", p.tableName)
	_, err := p.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

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
				id ulid PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	case "ulid_monotonic":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id ulid PRIMARY KEY,
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

func (p *PostgresBenchmarker) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

func WaitForReady() error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", dbHost, dbPort, dbUser, dbPassword, dbName)
	timeout := 30 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		db, err := sql.Open("postgres", connStr)
		if err == nil {
			if err := db.Ping(); err == nil {
				db.Close()
				return nil
			}
			db.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for PostgreSQL after %v", timeout)
}
