package statistics

import (
	"math"
	"sort"
)

// Stats holds statistical measures for a metric
type Stats struct {
	Median float64
	Mean   float64
	StdDev float64
	Min    float64
	Max    float64
	CV     float64 // Coefficient of Variation (%)
	Values []float64
}

// Median calculates the median of a slice of float64 values
func Median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	n := len(sorted)
	if n%2 == 0 {
		return (sorted[n/2-1] + sorted[n/2]) / 2.0
	}
	return sorted[n/2]
}

// Mean calculates the arithmetic mean
func Mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}

	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return sum / float64(len(values))
}

// StdDev calculates the sample standard deviation
func StdDev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}

	mean := Mean(values)
	variance := 0.0
	for _, v := range values {
		diff := v - mean
		variance += diff * diff
	}
	return math.Sqrt(variance / float64(len(values)-1))
}

// CV calculates the coefficient of variation (stddev/mean * 100)
func CV(values []float64) float64 {
	mean := Mean(values)
	if mean == 0 {
		return 0
	}
	return (StdDev(values) / math.Abs(mean)) * 100
}

// Calculate computes all statistical measures for a slice of values
func Calculate(values []float64) Stats {
	if len(values) == 0 {
		return Stats{}
	}

	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	// Store copy for later use (e.g., Mann-Whitney)
	valuesCopy := make([]float64, len(values))
	copy(valuesCopy, values)

	return Stats{
		Median: Median(values),
		Mean:   Mean(values),
		StdDev: StdDev(values),
		Min:    sorted[0],
		Max:    sorted[len(sorted)-1],
		CV:     CV(values),
		Values: valuesCopy,
	}
}

// HasOverlap checks if two value ranges overlap
func HasOverlap(statsA, statsB Stats) bool {
	// No overlap if: Min A > Max B OR Min B > Max A
	return !(statsA.Min > statsB.Max || statsB.Min > statsA.Max)
}
