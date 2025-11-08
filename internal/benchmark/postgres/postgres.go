package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

// PostgresBenchmarker implements the Benchmarker interface for PostgreSQL
type PostgresBenchmarker struct {
	db        *sql.DB
	keyType   string
	tableName string
	indexName string
}

// New creates a new PostgreSQL benchmarker instance
func New() *PostgresBenchmarker {
	return &PostgresBenchmarker{}
}
