package postgres

import (
	"database/sql"

	_ "github.com/lib/pq"
)

type PostgresBenchmarker struct {
	db        *sql.DB
	keyType   string
	tableName string
	indexName string
	startLSN  string // WAL LSN at start of insert operation
	endLSN    string // WAL LSN at end of insert operation
}

func New() *PostgresBenchmarker {
	return &PostgresBenchmarker{}
}
