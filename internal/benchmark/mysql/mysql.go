package mysql

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLBenchmarker struct {
	db                      *sql.DB
	keyType                 string
	tableName               string
	indexName               string
	startPageSplitCount     int64 // Global page split counter before operation
	endPageSplitCount       int64 // Global page split counter after operation
	pageSplitCountsCaptured bool  // Track if counts were captured
}

func New() *MySQLBenchmarker {
	return &MySQLBenchmarker{}
}
