package runner

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/cassandra"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

// PrepInsertConnections is the writer concurrency of the dataset bootstrap in
// the read and update scenarios. It is deliberately not the measured
// concurrency: the bootstrap only has to produce 50M rows, and at one writer
// it took roughly two hours per run, about two thirds of the whole June
// campaign. Eight writers cut that to about twenty minutes without changing
// what lands on disk (same rows, same size-triggered flush cadence, shared
// counter so sequential keys stay globally ascending), and it matches the
// concurrency the insert scenario itself already measures with.
const PrepInsertConnections = 8

// readSetFile is where the insert phase leaves the sampled target ids for the
// following read or update phase. In container mode the path lives inside the
// Cassandra container, which is destroyed after every run; in native mode it
// is on the orchestrator and is cleared before each insert. The orchestrator
// pid is part of the name so two campaigns started on the same machine cannot
// hand each other their read sets, which the key-type header would not catch
// when both benchmark the same key type.
var readSetFile = fmt.Sprintf("uuid-bench-read-set-%d.txt", os.Getpid())

// runSeed is the seed the next scenario derives its read-set sample and
// shuffle from. It is package state because the scenario functions are reached
// through the orchestrator's shared dbConfig closures, which carry no run
// index; SetRunSeed is called once per run, from a single goroutine, before
// the scenario starts.
var runSeed int64

// SetRunSeed fixes the read-set seed for the next scenario invocation. The
// orchestrator derives it from the campaign seed, the scenario, the key type
// and the repetition so every run draws its own sample and every sample can be
// reconstructed afterwards from the manifest.
func SetRunSeed(seed int64) { runSeed = seed }

// readSetPath returns the id-file path for the given execution mode, and
// clears any leftover file when the orchestrator owns the filesystem. A stale
// file from an aborted run would otherwise be picked up by a repeat of the
// same key type, which loadIDFile's key-type check cannot catch.
func readSetPath(mode workload.ExecutionMode) (string, error) {
	if mode == workload.ExecutionModeContainer {
		return "/tmp/" + readSetFile, nil
	}
	path := filepath.Join(os.TempDir(), readSetFile)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("clear stale read set %s: %w", path, err)
	}
	return path, nil
}

// sameReadSet checks that the measured phase loaded exactly the file the
// insert wrote. Both phases digest the bytes independently, so this catches a
// stale, edited or truncated read set that still parses. A no-op on the
// head-sampling path, where no file is involved.
func sameReadSet(idFile string, wrote, read *workload.WorkloadResult) error {
	if idFile == "" {
		return nil
	}
	if wrote.IDFileSHA256 == "" || read.IDFileSHA256 == "" {
		return fmt.Errorf("read set %s: missing digest (insert %q, measured phase %q)", idFile, wrote.IDFileSHA256, read.IDFileSHA256)
	}
	if wrote.IDFileSHA256 != read.IDFileSHA256 {
		return fmt.Errorf("read set %s changed between phases: insert wrote %s, measured phase read %s", idFile, wrote.IDFileSHA256, read.IDFileSHA256)
	}
	return nil
}

// cleanupReadSet removes the handed-over id file once the measured phase is
// done. Only meaningful in native mode: in container mode the file dies with
// the container. Without it every campaign process leaves its last read set
// behind, and an aborted one leaves it too.
func cleanupReadSet(mode workload.ExecutionMode, path string) {
	if path == "" || mode == workload.ExecutionModeContainer {
		return
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		fmt.Printf("Warning: could not remove read set %s: %v\n", path, err)
	}
}

// countsFrom converts the workload binary's tallies into the result struct.
// Attempted is the binary's own op count rather than the requested number so a
// short run is visible instead of being papered over.
func countsFrom(w *workload.WorkloadResult) benchmark.OperationCounts {
	return benchmark.OperationCounts{
		Attempted: w.TotalOps,
		Succeeded: w.TotalOps - w.Errors - w.NotFound,
		Failed:    w.Errors,
		NotFound:  w.NotFound,
	}
}

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

	wlResult, err := bench.InsertRecords(keyType, numRecords, batchSize, connections, prep.connStr, prep.execMode, string(cfg.Consistency), "", 0, 0)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}

	result.Counts = countsFrom(wlResult)
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
		result.IOValid = ioMetrics.Valid
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

	idFile, sampleSize, err := readSetFor(cfg, prep.execMode, numReads, numRecords)
	if err != nil {
		return nil, err
	}
	defer cleanupReadSet(prep.execMode, idFile)

	fmt.Printf("Inserting %d records to create data...\n", numRecords)
	insertResult, err := bench.InsertRecords(keyType, numRecords, 100, PrepInsertConnections, prep.connStr, prep.execMode, string(cfg.Consistency), idFile, sampleSize, runSeed)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertResult.Duration
	result.InsertFailed = insertResult.Errors
	result.IDFileSHA256 = insertResult.IDFileSHA256
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertResult.Duration)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	// The SSTable count and table size as the read phase finds them. Both are
	// how a compaction backlog left over from the bootstrap becomes visible in
	// the data instead of having to be argued about.
	result.TableSize = metrics.TableSize
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

	readResult, err := bench.ReadRecords(keyType, numReads, 1, prep.connStr, prep.execMode, string(cfg.Consistency), idFile)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	// The phase that measured must be the phase whose targets were drawn.
	// Both sides digest the file independently; a mismatch means the read set
	// was replaced or edited between them, which would otherwise produce
	// perfectly plausible numbers against the wrong rows.
	if err := sameReadSet(idFile, insertResult, readResult); err != nil {
		return nil, err
	}
	result.Counts = countsFrom(readResult)
	result.FetchDuration = readResult.FetchDuration
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
		result.IOValid = ioMetrics.Valid
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

	idFile, sampleSize, err := readSetFor(cfg, prep.execMode, numUpdates, numRecords)
	if err != nil {
		return nil, err
	}
	defer cleanupReadSet(prep.execMode, idFile)

	fmt.Printf("Inserting %d records...\n", numRecords)
	insertResult, err := bench.InsertRecords(keyType, numRecords, 100, PrepInsertConnections, prep.connStr, prep.execMode, string(cfg.Consistency), idFile, sampleSize, runSeed)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertResult.Duration
	result.InsertFailed = insertResult.Errors
	result.IDFileSHA256 = insertResult.IDFileSHA256
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

	updateResult, err := bench.UpdateRecords(keyType, numUpdates, 1, prep.connStr, prep.execMode, string(cfg.Consistency), idFile)
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}
	if err := sameReadSet(idFile, insertResult, updateResult); err != nil {
		return nil, err
	}
	result.Counts = countsFrom(updateResult)
	result.FetchDuration = updateResult.FetchDuration

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
		result.IOValid = ioMetrics.Valid
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
	// Mixed keeps the legacy in-phase fetch: it inserts while it reads, so a
	// read set drawn from the bootstrap alone would not cover the rows the
	// workload itself creates. Mixed is not part of the correction campaign.
	if _, err := bench.InsertRecords(keyType, initialDataset, 100, 1, prep.connStr, prep.execMode, string(cfg.Consistency), "", 0, 0); err != nil {
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

// readSetFor decides where the read or update phase gets its target ids. With
// the default sampler the insert draws them uniformly over insert order and
// hands them over in a file, so the measured phase issues no fetch at all;
// with HeadSampling the file is skipped and the legacy partition-head fetch
// runs inside the measured window, as it did in June.
func readSetFor(cfg cluster.ClusterConfig, mode workload.ExecutionMode, numOps, numRecords int) (string, int, error) {
	if cfg.HeadSampling {
		return "", 0, nil
	}
	// One target per operation, each drawn from a distinct row. Asking for
	// more operations than there are rows has no uniform answer, and silently
	// reusing targets would turn the measurement into a cache benchmark.
	if numOps > numRecords {
		return "", 0, fmt.Errorf("read set: -num-ops %d exceeds -num-records %d; the target set cannot cover one distinct row per operation", numOps, numRecords)
	}
	path, err := readSetPath(mode)
	if err != nil {
		return "", 0, err
	}
	return path, numOps, nil
}
