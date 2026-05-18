package runner

import (
	"fmt"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/cassandra"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

// scenarioPrep holds the per-scenario derived values that every Cassandra
// runner needs: gocql contact string, workload execution mode, and the
// io-metrics NodeRef slice. Centralising the derivation prevents drift
// across the five scenario functions.
type scenarioPrep struct {
	connStr    string
	execMode   workload.ExecutionMode
	hostsForIO []iometrics.NodeRef
}

// prepareScenario builds the values every Cassandra runner needs. Fail-fast
// on NodeContainerIDs errors — IO metrics depend on the IDs, and a missing
// container almost always indicates the cluster isn't actually up.
func prepareScenario(cfg cluster.ClusterConfig, backend cluster.Backend) (scenarioPrep, error) {
	ids, err := backend.NodeContainerIDs()
	if err != nil {
		return scenarioPrep{}, fmt.Errorf("node container ids: %w", err)
	}
	refs, err := buildNodeRefs(cfg, ids)
	if err != nil {
		return scenarioPrep{}, err
	}
	return scenarioPrep{
		connStr:    strings.Join(backend.NodeAddresses(), ","),
		execMode:   executionModeFor(cfg.Mode),
		hostsForIO: refs,
	}, nil
}

// executionModeFor maps a cluster.Mode to the workload binary's execution
// mode. Single-node uses the in-container path (the workload binary is
// docker-cp'd into the existing container). Local-cluster and remote-cluster
// both run the workload natively on the orchestrator and connect over the
// network — see Task 2.4 for the ExecutionMode contract.
func executionModeFor(m cluster.Mode) workload.ExecutionMode {
	if m == cluster.ModeLocalSingle {
		return workload.ExecutionModeContainer
	}
	return workload.ExecutionModeNative
}

// buildNodeRefs constructs the iometrics.NodeRef slice used to capture
// IO across the cluster. For ModeRemoteCluster each ref includes the
// SSH hostname; for local modes the host is empty (read cgroup directly
// on the orchestrator). Returns an error if the remote-cluster hostname
// list disagrees with the discovered container ID count — that mismatch
// would otherwise panic on the index dereference below.
func buildNodeRefs(cfg cluster.ClusterConfig, ids []string) ([]iometrics.NodeRef, error) {
	if cfg.Mode == cluster.ModeRemoteCluster {
		if len(cfg.Hostnames) != len(ids) {
			return nil, fmt.Errorf("remote cluster: %d hostnames vs %d container ids", len(cfg.Hostnames), len(ids))
		}
		out := make([]iometrics.NodeRef, len(cfg.Hostnames))
		for i, h := range cfg.Hostnames {
			out[i] = iometrics.NodeRef{Host: h, ContainerID: ids[i]}
		}
		return out, nil
	}
	out := make([]iometrics.NodeRef, len(ids))
	for i, id := range ids {
		out[i] = iometrics.NodeRef{Host: "", ContainerID: id}
	}
	return out, nil
}

func CassandraInsertPerformance(keyType string, numRecords, batchSize, connections int, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.InsertPerformanceResult, error) {
	bench := cassandra.New(cfg)

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	prep, err := prepareScenario(cfg, backend)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Inserting %d records (connections=%d, batch=%d)...\n", numRecords, connections, batchSize)

	result := &benchmark.InsertPerformanceResult{
		KeyType:     keyType,
		NumRecords:  numRecords,
		BatchSize:   batchSize,
		Connections: connections,
	}

	// Capture cluster-wide before-snapshot. The runner owns capture timing
	// so the snapshot pairs symmetrically with MeasureMetricsAll below.
	// Fail loud — a partial/missing before-snapshot would silently produce
	// zero PageSplits/BloomFilterFP deltas in the CSV with no operator-
	// visible signal that the measurement was incomplete.
	if err := bench.CaptureMetricsBeforeAll(backend); err != nil {
		return nil, fmt.Errorf("capture metrics before insert: %w", err)
	}

	ioStatsBefore, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before insert: %v\n", err)
	}

	wlResult, err := bench.InsertRecords(keyType, numRecords, batchSize, connections, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}

	result.Duration = wlResult.Duration
	result.Throughput = wlResult.Throughput
	result.LatencyP50 = wlResult.LatencyP50
	result.LatencyP95 = wlResult.LatencyP95
	result.LatencyP99 = wlResult.LatencyP99

	ioStatsAfter, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after insert: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Inserted %d records in %s\n", numRecords, result.Duration)
	fmt.Printf("Throughput: %.2f records/sec\n", result.Throughput)

	fmt.Println("Measuring metrics...")
	metrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result.PageSplits = metrics.PageSplits
	result.TableSize = metrics.TableSize
	result.IndexSize = metrics.IndexSize
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

func CassandraReadPerformance(keyType string, numRecords, numReads int, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.ReadPerformanceResult, error) {
	bench := cassandra.New(cfg)

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	prep, err := prepareScenario(cfg, backend)
	if err != nil {
		return nil, err
	}

	result := &benchmark.ReadPerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumReads:   numReads,
	}

	fmt.Printf("Inserting %d records to create data...\n", numRecords)
	insertResult, err := bench.InsertRecords(keyType, numRecords, 100, 1, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertResult.Duration
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertResult.Duration)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("SSTable count: %d\n", metrics.Fragmentation.LeafPages)

	// Capture metrics before read phase. Fail loud — see insert path.
	if err := bench.CaptureMetricsBeforeAll(backend); err != nil {
		return nil, fmt.Errorf("capture metrics before reads: %w", err)
	}

	fmt.Printf("Running %d point lookups...\n", numReads)

	ioStatsBefore, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before reads: %v\n", err)
	}

	readResult, err := bench.ReadRecords(keyType, numReads, 1, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readResult.Duration
	result.ReadThroughput = readResult.Throughput
	result.LatencyP50 = readResult.LatencyP50
	result.LatencyP95 = readResult.LatencyP95
	result.LatencyP99 = readResult.LatencyP99

	ioStatsAfter, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after reads: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Completed %d reads in %s\n", numReads, readResult.Duration)
	fmt.Printf("Read throughput: %.2f ops/sec\n", result.ReadThroughput)

	fmt.Println("Measuring cache hit ratios...")
	finalMetrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure final metrics: %w", err)
	}
	result.BufferHitRatio = finalMetrics.BufferHitRatio
	result.IndexBufferHitRatio = finalMetrics.IndexBufferHitRatio
	// Bloom filter FP delta is only tracked for the read scenario (not mixed/update)
	// because concurrent writes would flush memtables and change SSTable layout,
	// muddying the signal.
	result.BloomFilterFP = finalMetrics.BloomFilterFP

	return result, nil
}

func CassandraUpdatePerformance(keyType string, numRecords, numUpdates, batchSize int, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.UpdatePerformanceResult, error) {
	bench := cassandra.New(cfg)

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	prep, err := prepareScenario(cfg, backend)
	if err != nil {
		return nil, err
	}

	result := &benchmark.UpdatePerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumUpdates: numUpdates,
		BatchSize:  batchSize,
	}

	fmt.Printf("Inserting %d records...\n", numRecords)
	_, err = bench.InsertRecords(keyType, numRecords, 100, 1, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("Inserted %d records\n", numRecords)

	// Fail loud — see insert path.
	if err := bench.CaptureMetricsBeforeAll(backend); err != nil {
		return nil, fmt.Errorf("capture metrics before updates: %w", err)
	}

	fmt.Printf("Running %d updates...\n", numUpdates)

	ioStatsBefore, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before updates: %v\n", err)
	}

	updateResult, err := bench.UpdateRecords(keyType, numUpdates, 1, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	ioStatsAfter, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after updates: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	result.UpdateDuration = updateResult.Duration
	result.UpdateThroughput = updateResult.Throughput
	result.LatencyP50 = updateResult.LatencyP50
	result.LatencyP95 = updateResult.LatencyP95
	result.LatencyP99 = updateResult.LatencyP99

	fmt.Printf("Completed %d updates in %s\n", numUpdates, updateResult.Duration)
	fmt.Printf("Update throughput: %.2f ops/sec\n", result.UpdateThroughput)

	fmt.Println("Measuring metrics...")
	metrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

func CassandraMixedWorkloadInsertHeavy(keyType string, totalOps, connections, batchSize int, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.MixedWorkloadResult, error) {
	return runCassandraMixed(keyType, 100000, totalOps, connections, 70, 30, 0, "Insert-Heavy (70% insert, 30% read)", cfg, backend)
}

func CassandraMixedWorkloadReadUpdate(keyType string, totalOps, connections int, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.MixedWorkloadResult, error) {
	return runCassandraMixed(keyType, 500000, totalOps, connections, 0, 50, 50, "YCSB-A (50% read, 50% update)", cfg, backend)
}

// runCassandraMixed is the shared body of the two mixed-workload runners.
// It owns the full lifecycle: connect/create-table, bootstrap the initial
// dataset, capture before-snapshots, run the mixed workload, capture
// after-snapshots, and measure. Pulling the dataset bootstrap and
// before-capture up to the runner (out of bench.RunMixedWorkload) means
// both mixed scenarios pair a cluster-wide before-snapshot against the
// cluster-wide after-snapshot — matching the symmetry now in place for
// insert/read/update.
func runCassandraMixed(keyType string, initialDataset, totalOps, connections, insertW, readW, updateW int, label string, cfg cluster.ClusterConfig, backend cluster.Backend) (*benchmark.MixedWorkloadResult, error) {
	bench := cassandra.New(cfg)

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	prep, err := prepareScenario(cfg, backend)
	if err != nil {
		return nil, err
	}

	fmt.Printf("\n=== Mixed Workload: %s - %s ===\n", label, keyType)

	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	if _, err := bench.InsertRecords(keyType, initialDataset, 100, 1, prep.connStr, prep.execMode, string(cfg.Consistency)); err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	// Capture cluster-wide before-snapshot after the dataset bootstrap so
	// the mixed-workload deltas exclude the bootstrap's IO and compaction.
	// Fail loud — see insert path.
	if err := bench.CaptureMetricsBeforeAll(backend); err != nil {
		return nil, fmt.Errorf("capture metrics before mixed workload: %w", err)
	}

	ioStatsBefore, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before mixed workload: %v\n", err)
	}

	wlResult, err := bench.RunMixedWorkload(keyType, totalOps, connections, insertW, readW, updateW, prep.connStr, prep.execMode, string(cfg.Consistency))
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	ioStatsAfter, err := iometrics.GetClusterIOStats(prep.hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after mixed workload: %v\n", err)
	}

	metrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result := mixedResultFromWorkload(keyType, initialDataset, totalOps, wlResult, metrics, insertW, readW, updateW)

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Cache hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}
