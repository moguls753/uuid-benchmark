package statistics

import (
	"math"
	"testing"
)

func floatEquals(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

func assertFloat(t *testing.T, name string, got, want, epsilon float64) {
	t.Helper()
	if !floatEquals(got, want, epsilon) {
		t.Errorf("%s = %v, want %v (epsilon %v)", name, got, want, epsilon)
	}
}

func TestMedian(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"empty slice", nil, 0},
		{"single value", []float64{42}, 42},
		{"odd length", []float64{3, 1, 2}, 2},
		{"even length", []float64{4, 1, 3, 2}, 2.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Median(tt.values)
			assertFloat(t, "Median", got, tt.want, 1e-9)
		})
	}
}

func TestMean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"empty slice", nil, 0},
		{"multiple values", []float64{2, 4, 6}, 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := Mean(tt.values)
			assertFloat(t, "Mean", got, tt.want, 1e-9)
		})
	}
}

func TestStdDev(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"empty slice", nil, 0},
		{"single value", []float64{42}, 0},
		{"identical values", []float64{5, 5, 5, 5}, 0},
		// Sample stddev of {2,4,4,4,5,5,7,9}: mean=5, sum_sq_diff=32, variance=32/7, stddev=sqrt(32/7)
		{"textbook sample stddev", []float64{2, 4, 4, 4, 5, 5, 7, 9}, 2.138089935299395},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := StdDev(tt.values)
			assertFloat(t, "StdDev", got, tt.want, 1e-9)
		})
	}
}

func TestCV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{"mean is zero no divide by zero", []float64{-1, 1}, 0},
		// CV = (stddev / |mean|) * 100; {2,4,4,4,5,5,7,9}: mean=5, stddev≈2.138, CV≈42.76%
		{"textbook example", []float64{2, 4, 4, 4, 5, 5, 7, 9}, 42.76179870598791},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := CV(tt.values)
			assertFloat(t, "CV", got, tt.want, 1e-9)
		})
	}
}

func TestCalculate(t *testing.T) {
	t.Parallel()

	t.Run("populates all fields", func(t *testing.T) {
		t.Parallel()
		values := []float64{10, 20, 30, 40, 50}
		s := Calculate(values)

		assertFloat(t, "Median", s.Median, 30, 1e-9)
		assertFloat(t, "Mean", s.Mean, 30, 1e-9)
		assertFloat(t, "Min", s.Min, 10, 1e-9)
		assertFloat(t, "Max", s.Max, 50, 1e-9)
		if s.StdDev <= 0 {
			t.Errorf("StdDev should be > 0, got %v", s.StdDev)
		}
		if s.CV <= 0 {
			t.Errorf("CV should be > 0, got %v", s.CV)
		}
		if len(s.Values) != 5 {
			t.Errorf("Values length = %d, want 5", len(s.Values))
		}
	})

	t.Run("preserves original slice order", func(t *testing.T) {
		t.Parallel()
		input := []float64{5, 3, 1, 4, 2}
		Calculate(input)
		expected := []float64{5, 3, 1, 4, 2}
		for i, v := range input {
			if v != expected[i] {
				t.Errorf("input[%d] = %v, want %v", i, v, expected[i])
			}
		}
	})
}

func TestHasOverlap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b Stats
		want bool
	}{
		{"overlapping", Stats{Min: 1, Max: 5}, Stats{Min: 3, Max: 7}, true},
		{"non-overlapping", Stats{Min: 1, Max: 3}, Stats{Min: 5, Max: 7}, false},
		{"touching at boundary", Stats{Min: 1, Max: 5}, Stats{Min: 5, Max: 10}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := HasOverlap(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("HasOverlap = %v, want %v", got, tt.want)
			}
		})
	}
}
