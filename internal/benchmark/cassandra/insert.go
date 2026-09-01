package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

// InsertRecords runs the insert workload binary against the configured
// cluster. The runner owns metric-capture timing (CaptureMetricsBeforeAll
// + MeasureMetricsAll); this method is purely the workload execution.
// See internal/runner/cassandra.go for the surrounding capture pattern.
// idFile, sampleSize and sampleSeed are the read-set handoff: when idFile is
// non-empty the insert draws sampleSize target ids uniformly over insert order
// and writes them there for the following read or update phase. Passing an
// empty idFile keeps the insert unchanged.
func (c *CassandraBenchmarker) InsertRecords(keyType string, numRecords, batchSize, connections int, connString string, execMode workload.ExecutionMode, consistency, idFile string, sampleSize int, sampleSeed int64) (*workload.WorkloadResult, error) {
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "insert",
		KeyType:          keyType,
		NumRecords:       numRecords,
		BatchSize:        batchSize,
		Threads:          connections,
		ConnectionString: connString,
		NumBuckets:       c.cfg.NumBuckets,
		Consistency:      consistency,
		IDFile:           idFile,
		SampleSize:       sampleSize,
		SampleSeed:       sampleSeed,
	}
	if execMode == workload.ExecutionModeContainer {
		cfg.ContainerName = ContainerName
	}
	return workload.Execute(cfg)
}
