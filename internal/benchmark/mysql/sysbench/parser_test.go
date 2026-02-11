package sysbench

import (
	"testing"
	"time"
)

const realisticSysbenchOutput = `sysbench 1.0.20 (using bundled LuaJIT 2.1.0-beta2)

Running the test with following options:
Number of threads: 4
Initializing random number generator from current time

Initializing worker threads...

Threads started!

SQL statistics:
    queries performed:
        read:                            0
        write:                           100000
        other:                           0
        total:                           100000
    transactions:                        100000 (12345.67 per sec.)
    queries:                             100000 (12345.67 per sec.)
    ignored errors:                      3      (0.04 per sec.)
    reconnects:                          0      (0.00 per sec.)

General statistics:
    total time:                          8.1000s
    total number of events:              100000

Latency (ms):
         min:                                    0.12
         avg:                                    0.32
         max:                                   15.67
         95th percentile:                        0.74
         sum:                                32000.00

Threads fairness:
    events (avg/stddev):           25000.0000/52.31
    execution time (avg/stddev):   8.0000/0.01`

func TestParseSysbenchOutput(t *testing.T) {
	t.Parallel()

	t.Run("full realistic output", func(t *testing.T) {
		t.Parallel()
		result, err := ParseSysbenchOutput(realisticSysbenchOutput, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.TotalEvents != 100000 {
			t.Errorf("TotalEvents = %d, want 100000", result.TotalEvents)
		}
		if result.TPS < 12345.66 || result.TPS > 12345.68 {
			t.Errorf("TPS = %v, want ~12345.67", result.TPS)
		}
		if result.QPS < 12345.66 || result.QPS > 12345.68 {
			t.Errorf("QPS = %v, want ~12345.67", result.QPS)
		}
		if result.Errors != 3 {
			t.Errorf("Errors = %d, want 3", result.Errors)
		}

		wantAvg := time.Duration(0.32 * float64(time.Millisecond))
		if result.LatencyAvg != wantAvg {
			t.Errorf("LatencyAvg = %v, want %v", result.LatencyAvg, wantAvg)
		}
		wantMin := time.Duration(0.12 * float64(time.Millisecond))
		if result.LatencyMin != wantMin {
			t.Errorf("LatencyMin = %v, want %v", result.LatencyMin, wantMin)
		}
		want95 := time.Duration(0.74 * float64(time.Millisecond))
		if result.Latency95 != want95 {
			t.Errorf("Latency95 = %v, want %v", result.Latency95, want95)
		}
	})

	t.Run("missing TPS returns error", func(t *testing.T) {
		t.Parallel()
		output := `General statistics:
    total time:                          1.0s
    total number of events:              0`
		_, err := ParseSysbenchOutput(output, "")
		if err == nil {
			t.Fatal("expected error for missing TPS")
		}
	})

	t.Run("TPS computed from events and time when transactions line missing", func(t *testing.T) {
		t.Parallel()
		output := `General statistics:
    total time:                          10.0000s
    total number of events:              50000

Latency (ms):
         min:                                    0.10
         avg:                                    0.20
         max:                                    5.00
         95th percentile:                        0.50
         sum:                                10000.00`
		result, err := ParseSysbenchOutput(output, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result.TPS < 4999.9 || result.TPS > 5000.1 {
			t.Errorf("TPS = %v, want 5000", result.TPS)
		}
	})
}

func TestParseLatencyHistogram(t *testing.T) {
	t.Parallel()

	t.Run("valid histogram", func(t *testing.T) {
		t.Parallel()
		output := `SQL statistics:
    transactions:                        10 (100.00 per sec.)

Latency histogram (values are in milliseconds)
       value  ------------- distribution ------------- count
       0.100 |****                                     2
       0.200 |********                                 4
       0.300 |****                                     2
       0.500 |**                                       1
       1.000 |**                                       1

Threads fairness:
    events (avg/stddev):           10.0/0.00`

		p50, p95, p99, err := ParseLatencyHistogram(output)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 10 values sorted: [0.1, 0.1, 0.2, 0.2, 0.2, 0.2, 0.3, 0.3, 0.5, 1.0]
		// p50 index = int(10*0.50) = 5 → 0.2ms
		// p95 index = int(10*0.95) = 9 → 1.0ms
		wantP50 := time.Duration(0.2 * float64(time.Millisecond))
		if p50 != wantP50 {
			t.Errorf("p50 = %v, want %v", p50, wantP50)
		}
		wantP95 := time.Duration(1.0 * float64(time.Millisecond))
		if p95 != wantP95 {
			t.Errorf("p95 = %v, want %v", p95, wantP95)
		}
		if p99 != wantP95 {
			t.Errorf("p99 = %v, want %v", p99, wantP95)
		}
	})

	t.Run("no histogram section returns error", func(t *testing.T) {
		t.Parallel()
		_, _, _, err := ParseLatencyHistogram("no histogram here")
		if err == nil {
			t.Fatal("expected error when no histogram present")
		}
	})
}
