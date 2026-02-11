package pgbench

import (
	"testing"
	"time"
)

const realisticPgbenchOutput = `pgbench (18.0)
transaction type: Custom query
scaling factor: 1
query mode: simple
number of clients: 4
number of threads: 4
maximum number of tries: 1
number of transactions per client: 25000
number of transactions actually processed: 100000/100000
latency average = 1.234 ms
latency stddev = 0.567 ms
initial connection time = 12.345 ms
tps = 3237.456789 (without initial connection time)
tps = 3200.123456 (including initial connection time)`

const pgbenchOutputWithPercentiles = `transaction type: Custom query
scaling factor: 1
query mode: simple
number of clients: 4
number of threads: 4
number of transactions per client: 25000
number of transactions actually processed: 100000/100000
latency average = 2.400 ms
latency stddev = 1.100 ms
percentile 50 = 1.100 ms
percentile 95 = 2.300 ms
percentile 99 = 4.200 ms
initial connection time = 10.000 ms
tps = 80000.123456 (without initial connection time)
tps = 79000.000000 (including initial connection time)`

func TestParsePgbenchOutput(t *testing.T) {
	t.Parallel()

	t.Run("full realistic output", func(t *testing.T) {
		t.Parallel()
		result, err := ParsePgbenchOutput(realisticPgbenchOutput, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.Transactions != 100000 {
			t.Errorf("Transactions = %d, want 100000", result.Transactions)
		}
		if result.TPS < 3237.45 || result.TPS > 3237.46 {
			t.Errorf("TPS = %v, want ~3237.456789", result.TPS)
		}
		if result.TPSIncludingSetup < 3200.12 || result.TPSIncludingSetup > 3200.13 {
			t.Errorf("TPSIncludingSetup = %v, want ~3200.123456", result.TPSIncludingSetup)
		}
		if result.LatencyAvg != 1234*time.Microsecond {
			t.Errorf("LatencyAvg = %v, want 1.234ms", result.LatencyAvg)
		}
		if result.LatencyStdDev != 567*time.Microsecond {
			t.Errorf("LatencyStdDev = %v, want 0.567ms", result.LatencyStdDev)
		}
		if result.Duration <= 0 {
			t.Error("Duration should be positive")
		}
		// No percentiles in this output, no container → should stay zero
		if result.P50 != 0 || result.P95 != 0 || result.P99 != 0 {
			t.Errorf("percentiles should be zero, got P50=%v P95=%v P99=%v",
				result.P50, result.P95, result.P99)
		}
	})

	t.Run("output with percentiles", func(t *testing.T) {
		t.Parallel()
		result, err := ParsePgbenchOutput(pgbenchOutputWithPercentiles, "")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result.P50 != 1100*time.Microsecond {
			t.Errorf("P50 = %v, want 1.1ms", result.P50)
		}
		if result.P95 != 2300*time.Microsecond {
			t.Errorf("P95 = %v, want 2.3ms", result.P95)
		}
		if result.P99 != 4200*time.Microsecond {
			t.Errorf("P99 = %v, want 4.2ms", result.P99)
		}
	})

	t.Run("missing TPS returns error", func(t *testing.T) {
		t.Parallel()
		output := `number of transactions actually processed: 100/100
latency average = 1.0 ms`
		_, err := ParsePgbenchOutput(output, "")
		if err == nil {
			t.Fatal("expected error for missing TPS")
		}
	})

	t.Run("missing transaction count returns error", func(t *testing.T) {
		t.Parallel()
		output := `latency average = 1.0 ms
tps = 5000.0 (without initial connection time)`
		_, err := ParsePgbenchOutput(output, "")
		if err == nil {
			t.Fatal("expected error for missing transaction count")
		}
	})
}

func TestParseLatency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		line    string
		want    time.Duration
		wantErr bool
	}{
		{"millisecond unit", "latency average = 1.234 ms", 1234 * time.Microsecond, false},
		{"microsecond unit", "latency average = 500.000 us", 500 * time.Microsecond, false},
		{"missing equals sign", "latency average 1.234 ms", 0, true},
		{"no unit", "latency average = 1.234", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseLatency(tt.line)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLatency error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && got != tt.want {
				t.Errorf("parseLatency = %v, want %v", got, tt.want)
			}
		})
	}
}
