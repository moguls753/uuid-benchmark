package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

// RunMixedWorkload runs the mixed (insert/read/update) workload binary
// against the configured cluster. The runner owns dataset bootstrap and
// metric-capture timing: it inserts the initial dataset via InsertRecords,
// calls CaptureMetricsBeforeAll(backend), then invokes this method, then
// calls MeasureMetricsAll(backend). See internal/runner/cassandra.go.
func (c *CassandraBenchmarker) RunMixedWorkload(keyType string, totalOps, connections int, insertWeight, readWeight, updateWeight int, connString string, execMode workload.ExecutionMode, consistency string) (*workload.WorkloadResult, error) {
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "mixed",
		KeyType:          keyType,
		NumOps:           totalOps,
		Threads:          connections,
		ConnectionString: connString,
		InsertPct:        insertWeight,
		ReadPct:          readWeight,
		UpdatePct:        updateWeight,
		NumBuckets:       c.cfg.NumBuckets,
		Consistency:      consistency,
	}
	if execMode == workload.ExecutionModeContainer {
		cfg.ContainerName = ContainerName
	}
	return workload.Execute(cfg)
}
