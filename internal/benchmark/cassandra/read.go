package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

// A non-empty idFile makes the phase read its target ids from that file
// instead of querying the database for them, which is what removes the fetch
// from the runner's measured I/O window.
func (c *CassandraBenchmarker) ReadRecords(keyType string, numOps, connections int, connString string, execMode workload.ExecutionMode, consistency, idFile string) (*workload.WorkloadResult, error) {
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "read",
		KeyType:          keyType,
		NumOps:           numOps,
		Threads:          connections,
		ConnectionString: connString,
		NumBuckets:       c.cfg.NumBuckets,
		Consistency:      consistency,
		IDFile:           idFile,
	}
	if execMode == workload.ExecutionModeContainer {
		cfg.ContainerName = ContainerName
	}
	return workload.Execute(cfg)
}
