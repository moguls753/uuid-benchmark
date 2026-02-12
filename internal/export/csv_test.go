package export

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

func makeStats(values ...float64) statistics.Stats {
	return statistics.Calculate(values)
}

func TestStatsToCSV_SingleScenario(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.csv")

	scenarios := []ScenarioStats{
		{
			Name: "insert_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {
					"throughput":   makeStats(100, 110, 105),
					"p99_latency_us": makeStats(500, 520, 510),
				},
				"uuidv4": {
					"throughput":   makeStats(80, 85, 82),
					"p99_latency_us": makeStats(600, 620, 610),
				},
			},
		},
	}

	keyTypes := []string{"sequential", "uuidv4"}
	err := StatsToCSV(scenarios, keyTypes, path)
	if err != nil {
		t.Fatalf("StatsToCSV failed: %v", err)
	}

	rows := readCSV(t, path)

	// Header + 2 key types * 2 metrics = 5 rows
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows (1 header + 4 data), got %d", len(rows))
	}

	// Verify header includes Scenario column
	header := rows[0]
	if header[0] != "Scenario" {
		t.Errorf("expected first header column 'Scenario', got %q", header[0])
	}
	if len(header) != 9 {
		t.Errorf("expected 9 header columns, got %d", len(header))
	}

	// Verify scenario name in data rows
	if rows[1][0] != "insert_performance" {
		t.Errorf("expected scenario 'insert_performance', got %q", rows[1][0])
	}

	// Verify key type is uppercased
	if rows[1][1] != "SEQUENTIAL" {
		t.Errorf("expected key type 'SEQUENTIAL', got %q", rows[1][1])
	}
}

func TestStatsToCSV_MultipleScenarios(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.csv")

	scenarios := []ScenarioStats{
		{
			Name: "insert_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {"throughput": makeStats(100)},
			},
		},
		{
			Name: "read_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {"read_throughput": makeStats(200)},
			},
		},
	}

	keyTypes := []string{"sequential"}
	err := StatsToCSV(scenarios, keyTypes, path)
	if err != nil {
		t.Fatalf("StatsToCSV failed: %v", err)
	}

	rows := readCSV(t, path)

	// Header + 1 key type * 1 metric * 2 scenarios = 3 rows
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	if rows[1][0] != "insert_performance" {
		t.Errorf("expected first data row scenario 'insert_performance', got %q", rows[1][0])
	}
	if rows[2][0] != "read_performance" {
		t.Errorf("expected second data row scenario 'read_performance', got %q", rows[2][0])
	}
}

func TestRawRunsToCSV_Padding(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.csv")

	scenarios := []ScenarioStats{
		{
			Name: "test_scenario",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {
					"throughput": makeStats(100, 110, 105),
					"p99_latency_us": makeStats(500), // only 1 run value
				},
			},
		},
	}

	keyTypes := []string{"sequential"}
	err := RawRunsToCSV(scenarios, keyTypes, path)
	if err != nil {
		t.Fatalf("RawRunsToCSV failed: %v", err)
	}

	rows := readCSV(t, path)

	// Header: Scenario, KeyType, Metric, Run1, Run2, Run3
	header := rows[0]
	if header[0] != "Scenario" {
		t.Errorf("expected first header column 'Scenario', got %q", header[0])
	}
	if len(header) != 6 { // 3 fixed + 3 runs (max)
		t.Fatalf("expected 6 header columns, got %d", len(header))
	}

	// The p99_latency_us row should have 1 value + 2 empty padding cells
	for _, row := range rows[1:] {
		if len(row) != len(header) {
			t.Errorf("row %v has %d columns, expected %d", row, len(row), len(header))
		}
		if row[2] == "p99_latency_us" {
			// Run1 should have value, Run2 and Run3 should be empty
			if row[3] == "" {
				t.Error("p99_latency_us Run1 should not be empty")
			}
			if row[4] != "" || row[5] != "" {
				t.Errorf("p99_latency_us should have empty padding for Run2/Run3, got %q/%q", row[4], row[5])
			}
		}
	}
}

func TestStatsToCSV_SingleRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "stats.csv")

	scenarios := []ScenarioStats{
		{
			Name: "insert_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {"throughput": makeStats(100)},
			},
		},
	}

	keyTypes := []string{"sequential"}
	err := StatsToCSV(scenarios, keyTypes, path)
	if err != nil {
		t.Fatalf("StatsToCSV failed: %v", err)
	}

	rows := readCSV(t, path)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	row := rows[1]
	// With single run: Median=Mean=Min=Max=100, StdDev=0, CV=0
	if row[3] != "100.00" { // Median
		t.Errorf("expected Median 100.00, got %q", row[3])
	}
	if row[5] != "0.00" { // StdDev
		t.Errorf("expected StdDev 0.00 for single run, got %q", row[5])
	}
	if row[8] != "0.00" { // CV
		t.Errorf("expected CV 0.00 for single run, got %q", row[8])
	}
}

func TestOrderedMetricKeys_LogicalGrouping(t *testing.T) {
	m := map[string]statistics.Stats{
		"write_iops":          {},
		"throughput":           {},
		"p99_latency_us":       {},
		"fragmentation":        {},
		"table_size_mb":        {},
		"read_iops":           {},
		"p50_latency_us":       {},
	}

	keys := orderedMetricKeys(m)

	expected := []string{
		"throughput",
		"p50_latency_us",
		"p99_latency_us",
		"fragmentation",
		"table_size_mb",
		"read_iops",
		"write_iops",
	}

	if len(keys) != len(expected) {
		t.Fatalf("expected %d keys, got %d: %v", len(expected), len(keys), keys)
	}

	for i, key := range keys {
		if key != expected[i] {
			t.Errorf("position %d: expected %q, got %q", i, expected[i], key)
		}
	}
}

func TestOrderedMetricKeys_UnknownMetricsAppended(t *testing.T) {
	m := map[string]statistics.Stats{
		"throughput":    {},
		"custom_metric": {},
		"another_one":   {},
	}

	keys := orderedMetricKeys(m)

	// throughput first (known), then unknown sorted alphabetically
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d", len(keys))
	}
	if keys[0] != "throughput" {
		t.Errorf("expected first key 'throughput', got %q", keys[0])
	}
	if keys[1] != "another_one" {
		t.Errorf("expected second key 'another_one', got %q", keys[1])
	}
	if keys[2] != "custom_metric" {
		t.Errorf("expected third key 'custom_metric', got %q", keys[2])
	}
}

func TestRawRunsToCSV_MultipleScenarios(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "raw.csv")

	scenarios := []ScenarioStats{
		{
			Name: "insert_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {"throughput": makeStats(100, 110)},
			},
		},
		{
			Name: "read_performance",
			Results: map[string]map[string]statistics.Stats{
				"sequential": {"read_throughput": makeStats(200, 210)},
			},
		},
	}

	keyTypes := []string{"sequential"}
	err := RawRunsToCSV(scenarios, keyTypes, path)
	if err != nil {
		t.Fatalf("RawRunsToCSV failed: %v", err)
	}

	rows := readCSV(t, path)

	// Header + 2 data rows (1 metric per scenario)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify scenario names
	if rows[1][0] != "insert_performance" {
		t.Errorf("expected scenario 'insert_performance', got %q", rows[1][0])
	}
	if rows[2][0] != "read_performance" {
		t.Errorf("expected scenario 'read_performance', got %q", rows[2][0])
	}

	// Verify run values
	if rows[1][3] != "100.00" && rows[1][3] != "110.00" {
		t.Errorf("unexpected Run1 value for throughput: %q", rows[1][3])
	}
}

func readCSV(t *testing.T, path string) [][]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open CSV: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	rows, err := reader.ReadAll()
	if err != nil {
		t.Fatalf("failed to read CSV: %v", err)
	}
	return rows
}
