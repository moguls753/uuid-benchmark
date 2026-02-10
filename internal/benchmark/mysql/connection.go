package mysql

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/go-sql-driver/mysql"
)

func init() {
	// Suppress MySQL driver error logs (e.g., "unexpected EOF" during container startup)
	_ = mysql.SetLogger(log.New(io.Discard, "", 0))
}

const (
	dbHost     = "localhost"
	dbPort     = "3307"
	dbUser     = "benchmark"
	dbPassword = "benchmark123"
	dbName     = "uuid_benchmark"
)

func (m *MySQLBenchmarker) Connect() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(time.Hour)

	m.db = db

	return nil
}

func (m *MySQLBenchmarker) CreateTable(keyType string) error {
	m.keyType = keyType
	m.tableName = fmt.Sprintf("bench_%s", keyType)
	m.indexName = "PRIMARY"

	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", m.tableName)
	_, err := m.db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	var createSQL string
	switch keyType {
	case "sequential":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGINT AUTO_INCREMENT PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	case "uuidv4":
		// UUIDv4 stored as BINARY(16) for efficiency
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BINARY(16) PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	case "uuidv7":
		// UUIDv7 stored as BINARY(16) for efficiency
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BINARY(16) PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	case "ulid":
		// ULID stored as BINARY(16) for efficiency (same size as UUID)
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BINARY(16) PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	case "ulid_monotonic":
		// Monotonic ULID stored as BINARY(16)
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BINARY(16) PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	case "uuidv1":
		// UUIDv1 stored as BINARY(16)
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BINARY(16) PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
			) ENGINE=InnoDB
		`, m.tableName)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	_, err = m.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

func (m *MySQLBenchmarker) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func WaitForReady() error {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", dbUser, dbPassword, dbHost, dbPort, dbName)
	timeout := 60 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		db, err := sql.Open("mysql", dsn)
		if err == nil {
			if err := db.Ping(); err == nil {
				db.Close()
				return nil
			}
			db.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for MySQL after %v", timeout)
}
