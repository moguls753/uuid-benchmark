package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
	"github.com/moguls753/uuid-benchmark/internal/container"
	"github.com/moguls753/uuid-benchmark/internal/display"
	"github.com/moguls753/uuid-benchmark/internal/export"
	"github.com/moguls753/uuid-benchmark/internal/runner"
)

var allKeyTypes = []string{"sequential", "uuidv4", "uuidv7", "ulid", "ulid_monotonic", "uuidv1"}

// measuredConnections is the concurrency the read and update phases actually
// run at. All four database runners hardcode a single synchronous worker there
// regardless of -connections, so exporting the flag value in those scenarios'
// CSV rows would misdescribe the measurement.
const measuredConnections = 1

// campaignSeed drives both the key-type execution order and the per-run read-
// set draw. Zero keeps the historical fixed order, which is what reproduces
// pre-2026-09 runs; any other value randomises the order per repetition so a
// drift over the campaign cannot be mistaken for a key-type effect.
var campaignSeed int64

// campaignOutput is the -output path. When set, every finished run is appended
// to <output>.runs.jsonl and <output>.meta.json is rewritten, so a campaign
// that dies on run 40 of 90 still leaves everything it measured plus the
// provenance of how it was measured.
var campaignOutput string

var campaignManifest manifest

type manifest struct {
	Commit                string            `json:"commit"`
	WorkingTreeDirty      bool              `json:"working_tree_dirty"`
	OrchestratorMD5       string            `json:"orchestrator_md5"`
	WorkloadMD5           string            `json:"workload_md5"`
	Flags                 map[string]string `json:"flags"`
	CampaignSeed          int64             `json:"campaign_seed"`
	PrepInsertConnections int               `json:"prep_insert_connections"`
	MeasuredConnections   int               `json:"measured_connections_read_update"`
	Started               time.Time         `json:"started"`
	Updated               time.Time         `json:"updated"`
	Runs                  []runRecord       `json:"runs"`
}

type runRecord struct {
	Scenario   string    `json:"scenario"`
	KeyType    string    `json:"key_type"`
	Run        int       `json:"run"`
	OrderIndex int       `json:"order_index"`
	Seed       int64     `json:"sample_seed"`
	OrderSeed  int64     `json:"order_seed"`
	ReadSet    string    `json:"read_set_sha256,omitempty"`
	Started    time.Time `json:"started"`
	Seconds    float64   `json:"seconds"`
}

// deriveSeed mixes the campaign seed with a run's coordinates so every run has
// its own reproducible seed. FNV-1a is enough here: the values only have to be
// distinct and recomputable from the manifest, not unpredictable.
func deriveSeed(parts ...string) int64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%d", campaignSeed)
	for _, part := range parts {
		h.Write([]byte{0})
		h.Write([]byte(part))
	}
	return int64(h.Sum64() &^ (1 << 63))
}

// executionOrder returns the key-type order for one repetition. With no
// campaign seed the historical fixed order is kept so older runs stay
// reproducible; otherwise each repetition gets its own permutation, which
// turns the campaign into a randomised block design and means an interrupted
// run leaves whole blocks rather than missing key types.
func executionOrder(scenario string, run int) ([]string, int64) {
	order := make([]string, len(allKeyTypes))
	copy(order, allKeyTypes)
	seed := deriveSeed("order", scenario, fmt.Sprintf("%d", run))
	if campaignSeed == 0 {
		return order, seed
	}
	rng := rand.New(rand.NewPCG(uint64(seed), 0x243f6a8885a308d3))
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	return order, seed
}

// recordRun appends one finished run to the campaign log and rewrites the
// manifest. Both are best-effort: a bookkeeping failure warns but never kills a
// campaign that is otherwise producing data.
func recordRun(rec runRecord, metrics map[string]statistics.Stats) {
	campaignManifest.Runs = append(campaignManifest.Runs, rec)
	if campaignOutput == "" {
		return
	}

	line := struct {
		runRecord
		Campaign time.Time          `json:"campaign_started"`
		Metrics  map[string]float64 `json:"metrics"`
	}{runRecord: rec, Campaign: campaignManifest.Started, Metrics: make(map[string]float64, len(metrics))}
	for name, stat := range metrics {
		line.Metrics[name] = stat.Median
	}
	if encoded, err := json.Marshal(line); err != nil {
		fmt.Printf("Warning: encode run record: %v\n", err)
	} else if f, err := os.OpenFile(campaignOutput+".runs.jsonl", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
		fmt.Printf("Warning: open run log: %v\n", err)
	} else {
		if _, err := f.Write(append(encoded, '\n')); err != nil {
			fmt.Printf("Warning: write run log: %v\n", err)
		}
		if err := f.Close(); err != nil {
			fmt.Printf("Warning: close run log: %v\n", err)
		}
	}

	if err := writeManifest(); err != nil {
		fmt.Printf("Warning: %v\n", err)
	}
}

// writeManifest rewrites the campaign manifest in full. It is the only place
// the seeds live, so at campaign start a failure here is fatal (see
// initManifest): a campaign that cannot record how it was run produces numbers
// nobody can defend afterwards. Per-run rewrites only warn, because losing the
// last few run records is better than killing a campaign that is otherwise
// producing data.
func writeManifest() error {
	if campaignOutput == "" {
		return nil
	}
	campaignManifest.Updated = time.Now()
	encoded, err := json.MarshalIndent(campaignManifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	if err := os.WriteFile(campaignOutput+".meta.json", encoded, 0o644); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}
	return nil
}

// initManifest captures everything needed to say afterwards which code
// produced a dataset: commit, whether the tree was dirty, the md5 of both
// binaries and every flag value. Reconstructing this after the fact for the
// June 2026 runs took a full audit, and parts of it stayed unprovable.
func initManifest(database string) {
	commit, dirty, err := gitState()
	if err != nil {
		log.Fatalf("Campaign provenance: cannot determine the source commit: %v", err)
	}
	orchestrator, err := os.Executable()
	if err != nil {
		log.Fatalf("Campaign provenance: cannot locate the orchestrator binary: %v", err)
	}
	orchestratorSum, err := fileMD5(orchestrator)
	if err != nil {
		log.Fatalf("Campaign provenance: cannot hash the orchestrator binary: %v", err)
	}
	campaignManifest = manifest{
		Commit:                commit,
		WorkingTreeDirty:      dirty,
		OrchestratorMD5:       orchestratorSum,
		Flags:                 flagDump(),
		CampaignSeed:          campaignSeed,
		PrepInsertConnections: runner.PrepInsertConnections,
		MeasuredConnections:   measuredConnections,
		Started:               time.Now(),
	}
	// The run log is append-only within a campaign, so a rerun into the same
	// -output must not continue the previous one: its records would outlive
	// the manifest that is rewritten alongside it and the two campaigns would
	// read as one. Moved aside rather than deleted, because the most likely
	// reason for a rerun is that the previous campaign died partway and its
	// log is the only record of what it managed to measure.
	if campaignOutput != "" {
		stamp := time.Now().Format("20060102-150405")
		for _, previous := range []string{campaignOutput + ".runs.jsonl", campaignOutput + ".meta.json"} {
			if _, err := os.Stat(previous); err != nil {
				continue
			}
			archived := fmt.Sprintf("%s.%s.bak", previous, stamp)
			// Fatal, not a warning: if the old run log survives, the new
			// campaign's records append to it while the manifest beside it is
			// replaced, and the two campaigns become one file that nobody can
			// separate afterwards.
			if err := os.Rename(previous, archived); err != nil {
				log.Fatalf("Cannot archive the previous %s: %v. Move it aside or choose another -output.", previous, err)
			}
			fmt.Printf("Previous %s archived as %s\n", previous, archived)
		}
	}

	if database != "postgres" {
		path, err := workload.BuildBinary()
		if err != nil {
			log.Fatalf("Campaign provenance: cannot build the workload binary: %v", err)
		}
		sum, err := fileMD5(path)
		if err != nil {
			log.Fatalf("Campaign provenance: cannot hash the workload binary: %v", err)
		}
		campaignManifest.WorkloadMD5 = sum
	}

	// Fail here rather than 78 hours later: if the manifest cannot be written
	// at all (missing directory, no permission, full disk), every run that
	// follows would produce numbers whose provenance is unrecoverable.
	if err := writeManifest(); err != nil {
		log.Fatalf("Campaign provenance cannot be written: %v", err)
	}
}

// readSetOf digs the read-set fingerprint out of whichever result type the
// scenario produced. Only read and update have one; a type switch keeps
// runScenario generic instead of threading the value through every
// aggregation signature.
func readSetOf(result any) string {
	switch r := result.(type) {
	case *benchmark.ReadPerformanceResult:
		return r.IDFileSHA256
	case *benchmark.UpdatePerformanceResult:
		return r.IDFileSHA256
	}
	return ""
}

// The provenance helpers return errors rather than blanks. An empty commit or
// a falsely clean working tree is worse than no manifest at all: it looks like
// an answer. initManifest turns any of these into an abort before the first
// container starts.
func fileMD5(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := md5.Sum(data)
	return hex.EncodeToString(sum[:]), nil
}

func gitState() (string, bool, error) {
	commit, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "", false, fmt.Errorf("git rev-parse HEAD: %w", err)
	}
	// A failing status must never read as clean.
	status, err := exec.Command("git", "status", "--porcelain").Output()
	if err != nil {
		return "", false, fmt.Errorf("git status: %w", err)
	}
	return strings.TrimSpace(string(commit)), len(strings.TrimSpace(string(status))) > 0, nil
}

func flagDump() map[string]string {
	out := make(map[string]string)
	flag.VisitAll(func(f *flag.Flag) { out[f.Name] = f.Value.String() })
	return out
}

// Database configuration
type dbConfig struct {
	name             string
	id               string // canonical lowercase name for display logic ("postgres", "mysql", "mongodb", "cassandra")
	start            func() error
	stop             func() error
	insertFunc       func(string, int, int, int) (*benchmark.InsertPerformanceResult, error)
	readFunc         func(string, int, int) (*benchmark.ReadPerformanceResult, error)
	updateFunc       func(string, int, int, int) (*benchmark.UpdatePerformanceResult, error)
	mixedInsertHeavy func(string, int, int, int) (*benchmark.MixedWorkloadResult, error)
	mixedReadUpdate  func(string, int, int) (*benchmark.MixedWorkloadResult, error)
}

var postgresDB = dbConfig{
	name:             "PostgreSQL",
	id:               "postgres",
	start:            func() error { container.Start(container.PostgresConfig); return nil },
	stop:             func() error { container.Stop(container.PostgresConfig.ComposeFile); return nil },
	insertFunc:       runner.InsertPerformance,
	readFunc:         runner.ReadPerformance,
	updateFunc:       runner.UpdatePerformance,
	mixedInsertHeavy: runner.MixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MixedWorkloadReadUpdate,
}

var mysqlDB = dbConfig{
	name:             "MySQL",
	id:               "mysql",
	start:            func() error { container.Start(container.MySQLConfig); return nil },
	stop:             func() error { container.Stop(container.MySQLConfig.ComposeFile); return nil },
	insertFunc:       runner.MySQLInsertPerformance,
	readFunc:         runner.MySQLReadPerformance,
	updateFunc:       runner.MySQLUpdatePerformance,
	mixedInsertHeavy: runner.MySQLMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MySQLMixedWorkloadReadUpdate,
}

var mongodbDB = dbConfig{
	name:             "MongoDB",
	id:               "mongodb",
	start:            func() error { container.Start(container.MongoDBConfig); return nil },
	stop:             func() error { container.Stop(container.MongoDBConfig.ComposeFile); return nil },
	insertFunc:       runner.MongoDBInsertPerformance,
	readFunc:         runner.MongoDBReadPerformance,
	updateFunc:       runner.MongoDBUpdatePerformance,
	mixedInsertHeavy: runner.MongoDBMixedWorkloadInsertHeavy,
	mixedReadUpdate:  runner.MongoDBMixedWorkloadReadUpdate,
}

// cassandraDBConfig builds a Cassandra dbConfig that closes over the cluster
// config and Backend, since runner.Cassandra* signatures take both but the
// shared dbConfig function types do not (the other databases don't use them).
func cassandraDBConfig(cfg cluster.ClusterConfig, backend cluster.Backend) dbConfig {
	return dbConfig{
		name: "Cassandra",
		id:   "cassandra",
		start: func() error {
			if err := backend.Start(); err != nil {
				return fmt.Errorf("backend start: %w", err)
			}
			if err := backend.WaitForReady(); err != nil {
				return fmt.Errorf("backend wait for ready: %w", err)
			}
			return nil
		},
		stop: func() error {
			err := backend.Stop()
			// The container is gone; the cached "we already docker cp'd the
			// binary into <name>" entry would otherwise short-circuit the next
			// copy and leave the fresh container with no /tmp/workload.
			workload.ResetCopyCache()
			if err != nil {
				return fmt.Errorf("backend stop: %w", err)
			}
			return nil
		},
		insertFunc: func(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
			return runner.CassandraInsertPerformance(keyType, numRecords, batchSize, connections, cfg, backend)
		},
		readFunc: func(keyType string, numRecords, numReads int) (*benchmark.ReadPerformanceResult, error) {
			return runner.CassandraReadPerformance(keyType, numRecords, numReads, cfg, backend)
		},
		updateFunc: func(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
			return runner.CassandraUpdatePerformance(keyType, numRecords, numUpdates, batchSize, cfg, backend)
		},
		mixedInsertHeavy: func(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
			return runner.CassandraMixedWorkloadInsertHeavy(keyType, totalOps, connections, batchSize, cfg, backend)
		},
		mixedReadUpdate: func(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
			return runner.CassandraMixedWorkloadReadUpdate(keyType, totalOps, connections, cfg, backend)
		},
	}
}

var currentDB dbConfig

func main() {
	database := flag.String("database", "postgres", "Database to benchmark (postgres, mysql, mongodb, cassandra)")
	scenario := flag.String("scenario", "insert-performance", "Scenario to run (insert-performance, read-performance, update-performance, mixed-insert-heavy, mixed-read-update, all)")
	numRecords := flag.Int("num-records", 100000, "Number of records for insert operations")
	numOps := flag.Int("num-ops", 10000, "Number of operations for read/update/mixed scenarios")
	connections := flag.Int("connections", 1, "Number of concurrent connections")
	batchSize := flag.Int("batch-size", 100, "Batch size for inserts/updates")
	numRuns := flag.Int("num-runs", 1, "Number of runs per UUID type (for statistical analysis)")
	seed := flag.Int64("campaign-seed", 0, "Seed for key-type execution order and read-set sampling. 0 keeps the historical fixed order; any other value randomises the order per repetition and is recorded in <output>.meta.json")
	headSampling := flag.Bool("head-sampling", false, "Cassandra: select read/update targets with the legacy per-partition-head fetch instead of drawing them uniformly during the insert. Only for the bridge arm that sizes the difference between the two samplers")
	numBuckets := flag.Int("num-buckets", 1000, "Number of Cassandra partition buckets")
	// Cassandra cluster topology flags — only consulted for -database=cassandra.
	clusterMode := flag.String("cluster-mode", "local-single", "Cassandra cluster mode: local-single, local-cluster, remote-cluster (Cassandra only)")
	nodes := flag.String("nodes", "", "Comma-separated hostnames for remote-cluster mode (e.g. taurus5,taurus6,taurus7)")
	sshUser := flag.String("ssh-user", "", "SSH user for remote-cluster mode")
	sshKey := flag.String("ssh-key", "", "SSH private key path for remote-cluster mode (default: ssh-agent / ~/.ssh/id_*)")
	replicationFactor := flag.Int("replication-factor", 0, "Cassandra replication factor (default: 1 for local-single, 3 for cluster modes)")
	consistency := flag.String("consistency", "", "CQL consistency level: one, local_one, local_quorum, quorum (default: local_one for local-single, local_quorum for cluster modes)")
	clusterNodeCount := flag.Int("cluster-nodes", 3, "Number of nodes for local-cluster mode (must match docker/docker-compose.cassandra-cluster.yml service count)")
	cassandraHeap := flag.String("cassandra-heap", "8G", "MAX_HEAP_SIZE for each Cassandra container (remote-cluster only; Taurus-sized default)")
	cassandraNewGen := flag.String("cassandra-newgen", "2G", "HEAP_NEWSIZE for each Cassandra container (remote-cluster only; must be <= cassandra-heap)")
	cassandraCPUs := flag.String("cassandra-cpus", "8", "docker --cpus value for each Cassandra container (remote-cluster only; rejected by docker if > host CPU count)")
	cassandraMemory := flag.String("cassandra-memory", "32g", "docker --memory value for each Cassandra container (remote-cluster only)")
	cassandraImage := flag.String("cassandra-image", "", "Cassandra image reference for remote-cluster mode (default cassandra:5). Pin a digest for a multi-day campaign so a registry update cannot swap the engine version mid-run; the value is recorded in <output>.meta.json")
	singleNode := flag.Bool("single-node", false, "Allow a remote-cluster run with exactly one node. Without it a single-entry -nodes list is rejected as a likely typo")
	output := flag.String("output", "", "Output CSV file for statistical results")
	flag.Parse()

	campaignSeed = *seed
	campaignOutput = *output

	if *headSampling && !isCassandra(*database) {
		log.Fatalf("-head-sampling is Cassandra-only (got -database=%s)", *database)
	}
	if *singleNode && !isCassandra(*database) {
		log.Fatalf("-single-node is Cassandra-only (got -database=%s)", *database)
	}
	if *cassandraImage != "" && !isCassandra(*database) {
		log.Fatalf("-cassandra-image is Cassandra-only (got -database=%s)", *database)
	}
	// Both are consumed only by the remote-cluster branch of
	// buildClusterConfig. Accepting them elsewhere would leave the local
	// compose files running their own floating image while the manifest's
	// flag dump records a pin that never reached a container.
	if *clusterMode != "remote-cluster" {
		if *cassandraImage != "" {
			log.Fatalf("-cassandra-image only applies to -cluster-mode=remote-cluster (got %s); the local compose files pin their own image", *clusterMode)
		}
		if *singleNode {
			log.Fatalf("-single-node only applies to -cluster-mode=remote-cluster (got %s)", *clusterMode)
		}
	}

	// Reject cluster-mode flags on non-Cassandra dispatch. Without this an
	// operator who copies a `-cluster-mode=remote-cluster -nodes=...`
	// invocation and just flips `-database=postgres` gets a silent
	// single-container postgres run on the orchestrator and wastes hours
	// realizing the cluster flags were ignored.
	if !isCassandra(*database) {
		var offending []string
		if *clusterMode != "local-single" {
			offending = append(offending, "-cluster-mode")
		}
		if *nodes != "" {
			offending = append(offending, "-nodes")
		}
		if *sshUser != "" {
			offending = append(offending, "-ssh-user")
		}
		if *sshKey != "" {
			offending = append(offending, "-ssh-key")
		}
		if *replicationFactor != 0 {
			offending = append(offending, "-replication-factor")
		}
		if *consistency != "" {
			offending = append(offending, "-consistency")
		}
		if *clusterNodeCount != 3 {
			offending = append(offending, "-cluster-nodes")
		}
		// -num-buckets is Cassandra-specific (only the Cassandra schema uses
		// the bucket partition key); reject when non-default for non-Cassandra.
		if *numBuckets != 1000 {
			offending = append(offending, "-num-buckets")
		}
		if len(offending) > 0 {
			log.Fatalf("cluster flags (%s) are only valid with -database=cassandra; got -database=%s",
				strings.Join(offending, ", "), *database)
		}
	}

	// Select database configuration
	switch strings.ToLower(*database) {
	case "postgres", "postgresql", "pg":
		currentDB = postgresDB
	case "mysql", "my":
		currentDB = mysqlDB
	case "mongodb", "mongo":
		currentDB = mongodbDB
		allKeyTypes = append(allKeyTypes, "objectid")
		buildWorkloadBinary()
	case "cassandra", "cass":
		if *clusterMode == "local-cluster" && *clusterNodeCount <= 0 {
			log.Fatalf("-cluster-nodes must be >= 1 (got %d)", *clusterNodeCount)
		}
		cfg, err := buildClusterConfig(*clusterMode, *nodes, *sshUser, *sshKey, *consistency, *replicationFactor, *numBuckets, *cassandraHeap, *cassandraNewGen, *cassandraCPUs, *cassandraMemory, *singleNode, *cassandraImage)
		if err != nil {
			log.Fatalf("Build cluster config: %v", err)
		}
		cfg.HeadSampling = *headSampling
		backend, err := buildBackend(cfg, *clusterNodeCount)
		if err != nil {
			log.Fatalf("Build backend: %v", err)
		}
		currentDB = cassandraDBConfig(cfg, backend)
		buildWorkloadBinary()
	default:
		log.Fatalf("Invalid database: %s (use 'postgres', 'mysql', 'mongodb', or 'cassandra')", *database)
	}

	fmt.Printf("UUID Benchmark - %s\n", currentDB.name)
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Database:     %s\n", currentDB.name)
	if currentDB.id == "cassandra" {
		fmt.Printf("Cluster mode: %s\n", *clusterMode)
	}
	fmt.Printf("Scenario:     %s\n", *scenario)
	fmt.Printf("Records:      %d\n", *numRecords)
	if *connections > 1 {
		fmt.Printf("Connections:  %d\n", *connections)
	}
	if *batchSize > 1 {
		fmt.Printf("Batch Size:   %d\n", *batchSize)
	}
	if *numRuns > 1 {
		fmt.Printf("Runs:         %d (statistical mode)\n", *numRuns)
	}
	if campaignSeed != 0 {
		fmt.Printf("Campaign seed: %d (randomised key-type order)\n", campaignSeed)
	}
	if currentDB.id == "cassandra" {
		sampler := "uniform over insert order"
		if *headSampling {
			sampler = "legacy partition heads (bridge arm)"
		}
		fmt.Printf("Read set:     %s\n", sampler)
	}
	fmt.Printf("Testing:      %v\n", allKeyTypes)
	fmt.Println(strings.Repeat("=", 70))

	initManifest(currentDB.id)
	fmt.Println()

	switch *scenario {
	case "insert-performance":
		runInsertPerformance(*numRecords, *batchSize, *connections, *numRuns, *output, currentDB.id)

	case "read-performance":
		runReadPerformance(*numRecords, *numOps, *connections, *numRuns, *output, currentDB.id)

	case "update-performance":
		runUpdatePerformance(*numRecords, *numOps, *batchSize, *connections, *numRuns, *output, currentDB.id)

	case "mixed-insert-heavy":
		runMixedWorkloadInsertHeavy(*numOps, *numRecords, *connections, *batchSize, *numRuns, *output, currentDB.id)

	case "mixed-read-update":
		runMixedWorkloadReadUpdate(*numOps, *numRecords, *connections, *numRuns, *output, currentDB.id)

	case "all":
		runAllScenarios(*numRecords, *numOps, *connections, *batchSize, *numRuns, *output, currentDB.id)

	default:
		log.Fatalf("Invalid scenario: %s", *scenario)
	}

	fmt.Println()
	fmt.Println("All scenarios completed successfully!")
}

// runScenario is the generic core loop for all benchmark scenarios.
// It iterates key types, runs the workload numRuns times with fresh containers,
// aggregates results, displays them, and optionally exports CSV.
func runScenario[R any](
	scenarioName string,
	numRuns int,
	outputFile string,
	recordCount int,
	connections int,
	runOne func(keyType string) (*R, error),
	aggregate func(runs []*R) map[string]statistics.Stats,
	displaySingle func(results map[string]*R),
	displayStats func(stats map[string]map[string]statistics.Stats),
) map[string]map[string]statistics.Stats {
	allRuns := make(map[string][]*R)
	consecutiveInvalid := 0

	// Repetition outermost, key types shuffled within it: every repetition is
	// a block, so a drift across the campaign hits all key types alike, and an
	// abort leaves whole blocks instead of the last key types missing entirely.
	for i := 0; i < numRuns; i++ {
		order, orderSeed := executionOrder(scenarioName, i)
		if numRuns > 1 {
			fmt.Printf("\nRepetition %d/%d, order: %s\n", i+1, numRuns, strings.Join(order, ", "))
			fmt.Println(strings.Repeat("=", 70))
		}

		for orderIndex, keyType := range order {
			fmt.Printf("\nTesting %s", strings.ToUpper(keyType))
			if numRuns > 1 {
				fmt.Printf(" (run %d/%d)", i+1, numRuns)
			}
			fmt.Println()
			fmt.Println(strings.Repeat("-", 70))

			runSeed := deriveSeed("sample", scenarioName, keyType, fmt.Sprintf("%d", i))
			runner.SetRunSeed(runSeed)
			startedAt := time.Now()

			if err := currentDB.start(); err != nil {
				// Best-effort cleanup: a partially-started compose stack
				// would otherwise leak until the next run's pre-teardown,
				// and for Cassandra the workload copy cache (cleared inside
				// stop()) would point at the now-gone container.
				if stopErr := currentDB.stop(); stopErr != nil {
					fmt.Printf("Warning: stop after start failure: %v\n", stopErr)
				}
				log.Fatalf("Run %d start failed for %s: %v", i+1, keyType, err)
			}

			result, err := runOne(keyType)
			if err != nil {
				if stopErr := currentDB.stop(); stopErr != nil {
					fmt.Printf("Warning: stop after failure: %v\n", stopErr)
				}
				log.Fatalf("Run %d failed for %s: %v", i+1, keyType, err)
			}

			allRuns[keyType] = append(allRuns[keyType], result)

			// Logged before the teardown: a container that refuses to stop
			// aborts the campaign, and the run it just finished is measured
			// data that should survive that.
			runMetrics := aggregate([]*R{result})
			if stat, ok := runMetrics["io_valid"]; ok && len(stat.Values) > 0 && stat.Values[0] < 1 {
				fmt.Printf("\n!  Run %d of %s: the I/O window was not captured; throughput and latency stand, the I/O endpoint is void\n", i+1, strings.ToUpper(keyType))
			}
			if reason := invalidReason(runMetrics); reason != "" {
				consecutiveInvalid++
				fmt.Printf("\n!! Run %d of %s is INVALID: %s\n", i+1, strings.ToUpper(keyType), reason)
				if consecutiveInvalid >= 2 {
					if stopErr := currentDB.stop(); stopErr != nil {
						fmt.Printf("Warning: stop after invalid runs: %v\n", stopErr)
					}
					log.Fatalf("Two consecutive invalid runs; stopping so the campaign does not spend days on data that will be discarded. Last reason: %s", reason)
				}
			} else {
				consecutiveInvalid = 0
			}

			recordRun(runRecord{
				Scenario:   scenarioName,
				KeyType:    keyType,
				Run:        i + 1,
				OrderIndex: orderIndex,
				Seed:       runSeed,
				OrderSeed:  orderSeed,
				ReadSet:    readSetOf(result),
				Started:    startedAt,
				Seconds:    time.Since(startedAt).Seconds(),
			}, runMetrics)

			if err := currentDB.stop(); err != nil {
				log.Fatalf("Run %d stop failed for %s: %v", i+1, keyType, err)
			}

			if numRuns > 1 {
				fmt.Println("done")
			}
		}
	}

	allStats := make(map[string]map[string]statistics.Stats)
	for _, keyType := range allKeyTypes {
		allStats[keyType] = aggregate(allRuns[keyType])
	}

	if numRuns == 1 {
		singleResults := make(map[string]*R)
		for k, runs := range allRuns {
			singleResults[k] = runs[0]
		}
		displaySingle(singleResults)
	} else {
		displayStats(allStats)
	}

	if outputFile != "" {
		exportCSV(scenarioName, allStats, outputFile, recordCount, connections)
	}

	return allStats
}

func runInsertPerformance(numRecords, batchSize, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"insert_performance", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.InsertPerformanceResult, error) {
			return currentDB.insertFunc(keyType, numRecords, batchSize, connections)
		},
		aggregateInsertPerformanceResults,
		func(results map[string]*benchmark.InsertPerformanceResult) {
			display.InsertPerformance(results, allKeyTypes, connections, batchSize, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.InsertPerformanceStatistics(stats, allKeyTypes, numRecords, connections, batchSize, numRuns, database)
		},
	)
}

func runReadPerformance(numRecords, numOps, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"read_performance", numRuns, outputFile, numRecords, measuredConnections,
		func(keyType string) (*benchmark.ReadPerformanceResult, error) {
			return currentDB.readFunc(keyType, numRecords, numOps)
		},
		aggregateReadPerformanceResults,
		func(results map[string]*benchmark.ReadPerformanceResult) {
			display.ReadPerformance(results, allKeyTypes, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Read Performance", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runUpdatePerformance(numRecords, numOps, batchSize, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"update_performance", numRuns, outputFile, numRecords, measuredConnections,
		func(keyType string) (*benchmark.UpdatePerformanceResult, error) {
			return currentDB.updateFunc(keyType, numRecords, numOps, batchSize)
		},
		aggregateUpdatePerformanceResults,
		func(results map[string]*benchmark.UpdatePerformanceResult) {
			display.UpdatePerformance(results, allKeyTypes, database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Update Performance", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runMixedWorkloadInsertHeavy(totalOps, numRecords, connections, batchSize, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"mixed_insert_heavy", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.MixedWorkloadResult, error) {
			return currentDB.mixedInsertHeavy(keyType, totalOps, connections, batchSize)
		},
		aggregateMixedWorkloadResults,
		func(results map[string]*benchmark.MixedWorkloadResult) {
			display.MixedWorkload(results, allKeyTypes, "Insert-Heavy (70% insert, 30% read)", database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Mixed Insert-Heavy (70% insert, 30% read)", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runMixedWorkloadReadUpdate(totalOps, numRecords, connections, numRuns int, outputFile, database string) map[string]map[string]statistics.Stats {
	return runScenario(
		"mixed_read_update", numRuns, outputFile, numRecords, connections,
		func(keyType string) (*benchmark.MixedWorkloadResult, error) {
			return currentDB.mixedReadUpdate(keyType, totalOps, connections)
		},
		aggregateMixedWorkloadResults,
		func(results map[string]*benchmark.MixedWorkloadResult) {
			display.MixedWorkload(results, allKeyTypes, "YCSB-A (50% read, 50% update)", database)
		},
		func(stats map[string]map[string]statistics.Stats) {
			display.ScenarioStatistics("Mixed YCSB-A (50% read, 50% update)", stats, allKeyTypes, numRuns, database)
		},
	)
}

func runAllScenarios(numRecords, numOps, connections, batchSize, numRuns int, output, database string) {
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Println("RUNNING ALL SCENARIOS - COMPREHENSIVE BENCHMARK SUITE")
	fmt.Println(strings.Repeat("=", 100))
	fmt.Println()

	startTime := time.Now()

	fmt.Println("\n[1/5] INSERT PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	insertStats := runInsertPerformance(numRecords, batchSize, connections, numRuns, "", database)

	fmt.Println("\n[2/5] READ PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	readStats := runReadPerformance(numRecords, numOps, connections, numRuns, "", database)

	fmt.Println("\n[3/5] UPDATE PERFORMANCE")
	fmt.Println(strings.Repeat("=", 100))
	updateStats := runUpdatePerformance(numRecords, numOps, batchSize, connections, numRuns, "", database)

	fmt.Println("\n[4/5] MIXED INSERT-HEAVY")
	fmt.Println(strings.Repeat("=", 100))
	mixedIHStats := runMixedWorkloadInsertHeavy(numOps, numRecords, connections, batchSize, numRuns, "", database)

	fmt.Println("\n[5/5] MIXED READ-UPDATE (YCSB-A)")
	fmt.Println(strings.Repeat("=", 100))
	mixedRUStats := runMixedWorkloadReadUpdate(numOps, numRecords, connections, numRuns, "", database)

	totalDuration := time.Since(startTime)
	fmt.Println("\n" + strings.Repeat("=", 100))
	fmt.Printf("ALL SCENARIOS COMPLETED IN %s\n", totalDuration.Round(time.Second))
	fmt.Println(strings.Repeat("=", 100))

	if output != "" {
		scenarios := []export.ScenarioStats{
			{Name: "insert_performance", RecordCount: numRecords, Connections: connections, Results: insertStats},
			{Name: "read_performance", RecordCount: numRecords, Connections: measuredConnections, Results: readStats},
			{Name: "update_performance", RecordCount: numRecords, Connections: measuredConnections, Results: updateStats},
			{Name: "mixed_insert_heavy", RecordCount: numRecords, Connections: connections, Results: mixedIHStats},
			{Name: "mixed_read_update", RecordCount: numRecords, Connections: connections, Results: mixedRUStats},
		}

		fmt.Printf("\nExporting results to CSV...\n")

		if err := export.StatsToCSV(scenarios, allKeyTypes, output); err != nil {
			log.Printf("Warning: Failed to export stats CSV: %v", err)
		} else {
			fmt.Printf("  Statistical summary: %s\n", output)
		}

		rawFile := strings.Replace(output, ".csv", "_raw.csv", 1)
		if rawFile == output {
			rawFile = output + ".raw"
		}
		if err := export.RawRunsToCSV(scenarios, allKeyTypes, rawFile); err != nil {
			log.Printf("Warning: Failed to export raw runs CSV: %v", err)
		} else {
			fmt.Printf("  Raw runs data: %s\n", rawFile)
		}
	}
}

// Aggregation functions

// countMetrics turns the per-run operation tallies into CSV series for the
// scenarios that carry them (insert, read, update; mixed has no counts).
// Throughput counts attempts, so without the success rate travelling alongside
// it a run that failed most of its operations is indistinguishable from a fast
// one.
func countMetrics(counts []benchmark.OperationCounts) map[string]statistics.Stats {
	// Only the Cassandra runners populate these today. Emitting all-zero
	// columns for the other databases would state that nothing was attempted,
	// which contradicts their own throughput in the same row.
	populated := false
	for _, c := range counts {
		if c.Attempted > 0 {
			populated = true
			break
		}
	}
	if !populated {
		return nil
	}

	attempted := make([]float64, len(counts))
	succeeded := make([]float64, len(counts))
	failed := make([]float64, len(counts))
	notFound := make([]float64, len(counts))
	for i, c := range counts {
		attempted[i] = float64(c.Attempted)
		succeeded[i] = float64(c.Succeeded)
		failed[i] = float64(c.Failed)
		notFound[i] = float64(c.NotFound)
	}
	return map[string]statistics.Stats{
		"attempted": statistics.Calculate(attempted),
		"succeeded": statistics.Calculate(succeeded),
		"failed":    statistics.Calculate(failed),
		"not_found": statistics.Calculate(notFound),
	}
}

// ioValidMetric turns the per-run capture flag into a CSV series. It travels
// with the I/O columns because a failed capture leaves them at zero, and zero
// is the most favourable value read_iops can take: without this column a
// missing measurement is indistinguishable from the strongest possible result.
func ioValidMetric(valid []bool) statistics.Stats {
	out := make([]float64, len(valid))
	for i, v := range valid {
		if v {
			out[i] = 1
		}
	}
	return statistics.Calculate(out)
}

// invalidReason judges a finished run against the same counters the analysis
// protocol uses, and returns why it is unusable or "" if it is fine. Exporting
// the counters is not enough on its own: a cluster that degrades at hour 30
// would otherwise keep producing runs that are all discarded later, and the
// campaign would spend the remaining days on data nobody can use.
// A failed I/O capture is deliberately not among the reasons: throughput and
// latency are measured independently of the cgroup window, so such a run still
// carries its primary numbers and only loses the I/O endpoint. It is reported
// through the io_valid column and warned about, but it must not count towards
// the abort, or a run of capture failures would throw away good measurements.
func invalidReason(metrics map[string]statistics.Stats) string {
	value := func(name string) (float64, bool) {
		stat, ok := metrics[name]
		if !ok || len(stat.Values) == 0 {
			return 0, false
		}
		return stat.Values[0], true
	}

	if failed, ok := value("insert_failed"); ok && failed > 0 {
		return fmt.Sprintf("the dataset bootstrap lost %.0f rows", failed)
	}
	attempted, ok := value("attempted")
	if !ok || attempted <= 0 {
		return ""
	}
	failed, _ := value("failed")
	notFound, _ := value("not_found")
	if rate := (failed + notFound) / attempted; rate >= 0.001 {
		return fmt.Sprintf("%.0f of %.0f operations failed or found nothing (%.3f %%)", failed+notFound, attempted, rate*100)
	}
	return ""
}

// mergeMetrics copies src into dst, failing loudly on a key collision so a
// renamed metric can never silently overwrite another one.
func mergeMetrics(dst, src map[string]statistics.Stats) {
	for k, v := range src {
		if _, clash := dst[k]; clash {
			log.Fatalf("duplicate metric key %q in aggregation", k)
		}
		dst[k] = v
	}
}

func aggregateInsertPerformanceResults(runs []*benchmark.InsertPerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	throughput := make([]float64, numRuns)
	pageSplits := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	avgLeafDensity := make([]float64, numRuns)
	tableSizeMB := make([]float64, numRuns)
	indexSizeMB := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)
	counts := make([]benchmark.OperationCounts, numRuns)
	ioValid := make([]bool, numRuns)

	for i, run := range runs {
		counts[i] = run.Counts
		ioValid[i] = run.IOValid
		throughput[i] = run.Throughput
		pageSplits[i] = float64(run.PageSplits)
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		avgLeafDensity[i] = run.Fragmentation.AvgLeafDensity
		tableSizeMB[i] = float64(run.TableSize) / (1024 * 1024)
		indexSizeMB[i] = float64(run.IndexSize) / (1024 * 1024)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	result := map[string]statistics.Stats{
		"throughput":          statistics.Calculate(throughput),
		"fragmentation":       statistics.Calculate(fragmentation),
		"avg_leaf_density":    statistics.Calculate(avgLeafDensity),
		"table_size_mb":       statistics.Calculate(tableSizeMB),
		"index_size_mb":       statistics.Calculate(indexSizeMB),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}

	mergeMetrics(result, countMetrics(counts))

	// Cassandra uses SSTable count delta; B-tree databases use page splits
	if currentDB.id == "cassandra" {
		result["io_valid"] = ioValidMetric(ioValid)
		result["sstable_count"] = statistics.Calculate(pageSplits)
	} else {
		result["page_splits"] = statistics.Calculate(pageSplits)
	}

	return result
}

func aggregateReadPerformanceResults(runs []*benchmark.ReadPerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	readThroughput := make([]float64, numRuns)
	cacheHitRatio := make([]float64, numRuns)
	indexHitRatio := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	bloomFilterFP := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	insertSeconds := make([]float64, numRuns)
	fetchSeconds := make([]float64, numRuns)
	insertFailed := make([]float64, numRuns)
	ioValid := make([]bool, numRuns)
	sstableCount := make([]float64, numRuns)
	tableSizeMB := make([]float64, numRuns)
	counts := make([]benchmark.OperationCounts, numRuns)

	for i, run := range runs {
		counts[i] = run.Counts
		insertSeconds[i] = run.InsertDuration.Seconds()
		fetchSeconds[i] = run.FetchDuration.Seconds()
		insertFailed[i] = float64(run.InsertFailed)
		ioValid[i] = run.IOValid
		sstableCount[i] = float64(run.Fragmentation.LeafPages)
		tableSizeMB[i] = float64(run.TableSize) / (1024 * 1024)
		readThroughput[i] = run.ReadThroughput
		cacheHitRatio[i] = run.BufferHitRatio
		indexHitRatio[i] = run.IndexBufferHitRatio
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		bloomFilterFP[i] = float64(run.BloomFilterFP)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	result := map[string]statistics.Stats{
		"read_throughput":     statistics.Calculate(readThroughput),
		"t_insert_s":          statistics.Calculate(insertSeconds),
		"cache_hit_ratio":     statistics.Calculate(cacheHitRatio),
		"index_hit_ratio":     statistics.Calculate(indexHitRatio),
		"fragmentation":       statistics.Calculate(fragmentation),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}

	mergeMetrics(result, countMetrics(counts))

	// Bloom filter FP and the SSTable count at read time are Cassandra-only
	// (LSM-tree SSTable lookups); the B-tree engines report LeafPages with a
	// different meaning.
	// Cassandra-only: the other runners neither time their id fetch, nor
	// report bootstrap row losses, nor fill the read-phase table size, and
	// none of them sets the I/O capture flag. Exporting these columns for
	// them would read as real zeros, and for io_valid a zero means the
	// capture failed.
	if currentDB.id == "cassandra" {
		result["t_fetch_s"] = statistics.Calculate(fetchSeconds)
		result["insert_failed"] = statistics.Calculate(insertFailed)
		result["table_size_mb"] = statistics.Calculate(tableSizeMB)
		result["io_valid"] = ioValidMetric(ioValid)
		result["bloom_filter_fp"] = statistics.Calculate(bloomFilterFP)
		result["sstable_count"] = statistics.Calculate(sstableCount)
	}

	return result
}

func aggregateUpdatePerformanceResults(runs []*benchmark.UpdatePerformanceResult) map[string]statistics.Stats {
	numRuns := len(runs)

	updateThroughput := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	insertSeconds := make([]float64, numRuns)
	fetchSeconds := make([]float64, numRuns)
	insertFailed := make([]float64, numRuns)
	ioValid := make([]bool, numRuns)
	counts := make([]benchmark.OperationCounts, numRuns)

	for i, run := range runs {
		counts[i] = run.Counts
		insertSeconds[i] = run.InsertDuration.Seconds()
		fetchSeconds[i] = run.FetchDuration.Seconds()
		insertFailed[i] = float64(run.InsertFailed)
		ioValid[i] = run.IOValid
		updateThroughput[i] = run.UpdateThroughput
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	result := map[string]statistics.Stats{
		"update_throughput":   statistics.Calculate(updateThroughput),
		"t_insert_s":          statistics.Calculate(insertSeconds),
		"fragmentation":       statistics.Calculate(fragmentation),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}

	// Cassandra-only, for the reasons given in the read aggregation.
	if currentDB.id == "cassandra" {
		result["t_fetch_s"] = statistics.Calculate(fetchSeconds)
		result["insert_failed"] = statistics.Calculate(insertFailed)
		result["io_valid"] = ioValidMetric(ioValid)
	}

	mergeMetrics(result, countMetrics(counts))

	return result
}

func aggregateMixedWorkloadResults(runs []*benchmark.MixedWorkloadResult) map[string]statistics.Stats {
	numRuns := len(runs)

	overallThroughput := make([]float64, numRuns)
	cacheHitRatio := make([]float64, numRuns)
	indexHitRatio := make([]float64, numRuns)
	fragmentation := make([]float64, numRuns)
	tableSizeMB := make([]float64, numRuns)
	indexSizeMB := make([]float64, numRuns)
	p50Latency := make([]float64, numRuns)
	p95Latency := make([]float64, numRuns)
	p99Latency := make([]float64, numRuns)
	readIOPS := make([]float64, numRuns)
	writeIOPS := make([]float64, numRuns)
	readThroughputMB := make([]float64, numRuns)
	writeThroughputMB := make([]float64, numRuns)

	for i, run := range runs {
		overallThroughput[i] = run.OverallThroughput
		cacheHitRatio[i] = run.BufferHitRatio
		indexHitRatio[i] = run.IndexBufferHitRatio
		fragmentation[i] = run.Fragmentation.FragmentationPercent
		tableSizeMB[i] = float64(run.TableSize) / (1024 * 1024)
		indexSizeMB[i] = float64(run.IndexSize) / (1024 * 1024)
		p50Latency[i] = float64(run.LatencyP50.Microseconds())
		p95Latency[i] = float64(run.LatencyP95.Microseconds())
		p99Latency[i] = float64(run.LatencyP99.Microseconds())
		readIOPS[i] = run.ReadIOPS
		writeIOPS[i] = run.WriteIOPS
		readThroughputMB[i] = run.ReadThroughputMB
		writeThroughputMB[i] = run.WriteThroughputMB
	}

	return map[string]statistics.Stats{
		"overall_throughput":  statistics.Calculate(overallThroughput),
		"cache_hit_ratio":     statistics.Calculate(cacheHitRatio),
		"index_hit_ratio":     statistics.Calculate(indexHitRatio),
		"fragmentation":       statistics.Calculate(fragmentation),
		"table_size_mb":       statistics.Calculate(tableSizeMB),
		"index_size_mb":       statistics.Calculate(indexSizeMB),
		"p50_latency_us":      statistics.Calculate(p50Latency),
		"p95_latency_us":      statistics.Calculate(p95Latency),
		"p99_latency_us":      statistics.Calculate(p99Latency),
		"read_iops":           statistics.Calculate(readIOPS),
		"write_iops":          statistics.Calculate(writeIOPS),
		"read_throughput_mb":  statistics.Calculate(readThroughputMB),
		"write_throughput_mb": statistics.Calculate(writeThroughputMB),
	}
}

// exportCSV exports stats for a single scenario to CSV files
func exportCSV(scenarioName string, allStats map[string]map[string]statistics.Stats, outputFile string, recordCount, connections int) {
	scenarios := []export.ScenarioStats{{Name: scenarioName, RecordCount: recordCount, Connections: connections, Results: allStats}}

	fmt.Printf("\nExporting results to CSV...\n")

	if err := export.StatsToCSV(scenarios, allKeyTypes, outputFile); err != nil {
		log.Printf("Warning: Failed to export stats CSV: %v", err)
	} else {
		fmt.Printf("  Statistical summary: %s\n", outputFile)
	}

	rawFile := strings.Replace(outputFile, ".csv", "_raw.csv", 1)
	if rawFile == outputFile {
		rawFile = outputFile + ".raw"
	}
	if err := export.RawRunsToCSV(scenarios, allKeyTypes, rawFile); err != nil {
		log.Printf("Warning: Failed to export raw runs CSV: %v", err)
	} else {
		fmt.Printf("  Raw runs data: %s\n", rawFile)
	}
}

// buildClusterConfig assembles a cluster.ClusterConfig from CLI flags,
// applies per-mode defaults for replication factor and consistency, sets
// NumBuckets, and runs cfg.Validate(). The single-helper contract means
// callers can't accidentally skip validation by reordering setup steps.
func buildClusterConfig(mode, nodesStr, sshUser, sshKey, consistency string, rf, numBuckets int, cassandraHeap, cassandraNewGen, cassandraCPUs, cassandraMemory string, singleNode bool, cassandraImage string) (cluster.ClusterConfig, error) {
	cfg := cluster.ClusterConfig{
		Keyspace:   "uuid_benchmark",
		Mode:       cluster.Mode(mode),
		NumBuckets: numBuckets,
	}
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		cfg.ContactPoints = []string{"127.0.0.1"}
		if rf == 0 {
			rf = 1
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalOne)
		}
	case cluster.ModeLocalCluster:
		cfg.ContactPoints = []string{"127.0.0.1"}
		if rf == 0 {
			rf = 3
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalQuorum)
		}
	case cluster.ModeRemoteCluster:
		hosts := parseHostList(nodesStr)
		if len(hosts) == 0 {
			return cfg, fmt.Errorf("remote-cluster mode requires -nodes (comma-separated hostnames)")
		}
		cfg.ContactPoints = hosts
		cfg.Hostnames = hosts
		cfg.SSHUser = sshUser
		cfg.SSHKeyPath = sshKey
		// Resource flags are only meaningful in remote-cluster mode —
		// LocalSingle and LocalCluster pin their sizing in the
		// docker-compose YAMLs. Wired here so Validate() can check the
		// heap/newGen invariant per ClusterConfig.
		cfg.CassandraHeap = cassandraHeap
		cfg.CassandraNewGen = cassandraNewGen
		cfg.CassandraCPUs = cassandraCPUs
		cfg.CassandraMemory = cassandraMemory
		cfg.CassandraImage = cassandraImage
		// Set before Validate below: it is the acknowledgement that lets a
		// one-host node list through.
		cfg.SingleNode = singleNode
		if rf == 0 {
			// A one-host cluster cannot replicate three ways, and requiring
			// the operator to say so twice would only produce a validation
			// error on the documented invocation.
			if singleNode && len(hosts) == 1 {
				rf = 1
			} else {
				rf = 3
			}
		}
		if consistency == "" {
			consistency = string(cluster.ConsistencyLocalQuorum)
		}
	default:
		return cfg, fmt.Errorf("unknown cluster mode %q (expected local-single, local-cluster, or remote-cluster)", mode)
	}
	cfg.ReplicationFactor = rf
	cfg.Consistency = cluster.Consistency(consistency)
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

// buildBackend constructs the cluster.Backend matching cfg.Mode. localNodeCount
// is only consulted in ModeLocalCluster.
func buildBackend(cfg cluster.ClusterConfig, localNodeCount int) (cluster.Backend, error) {
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		return cluster.NewLocalSingle(), nil
	case cluster.ModeLocalCluster:
		return cluster.NewLocalCluster(localNodeCount), nil
	case cluster.ModeRemoteCluster:
		return cluster.NewRemoteCluster(cfg), nil
	}
	return nil, fmt.Errorf("unknown cluster mode %q", cfg.Mode)
}

// parseHostList splits a comma-separated host list, trimming whitespace and
// dropping empty entries. Mirrors parseContactPoints in cmd/workload/main.go
// but lives locally since the workload binary's helper isn't importable.
func parseHostList(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// isCassandra reports whether the -database flag value names the Cassandra
// runner. Mirrors the aliases accepted in the dispatch switch.
func isCassandra(database string) bool {
	switch strings.ToLower(database) {
	case "cassandra", "cass":
		return true
	}
	return false
}

func buildWorkloadBinary() {
	fmt.Println("Building workload binary for NoSQL databases...")
	path, err := workload.BuildBinary()
	if err != nil {
		log.Fatalf("Failed to build workload binary: %v", err)
	}
	fmt.Printf("Workload binary built: %s\n", path)
}
