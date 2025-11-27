package statistics

import (
	"math"
	"sort"
)

// MannWhitneyU performs a Mann-Whitney U test on two groups
// Returns the p-value (approximate, using normal approximation)
// H0: The two groups come from the same distribution
// If p < 0.05: reject H0 (groups are significantly different)
func MannWhitneyU(groupA, groupB []float64) float64 {
	if len(groupA) == 0 || len(groupB) == 0 {
		return 1.0 // No difference if empty
	}

	n1 := len(groupA)
	n2 := len(groupB)

	// Combine and rank
	type rankItem struct {
		value float64
		group int // 0 for A, 1 for B
	}

	combined := make([]rankItem, n1+n2)
	for i, v := range groupA {
		combined[i] = rankItem{v, 0}
	}
	for i, v := range groupB {
		combined[n1+i] = rankItem{v, 1}
	}

	// Sort by value
	sort.Slice(combined, func(i, j int) bool {
		return combined[i].value < combined[j].value
	})

	// Assign ranks (handling ties)
	ranks := make([]float64, len(combined))
	for i := 0; i < len(combined); {
		j := i
		// Find end of tied values
		for j < len(combined) && combined[j].value == combined[i].value {
			j++
		}
		// Average rank for ties
		avgRank := float64(i+j+1) / 2.0
		for k := i; k < j; k++ {
			ranks[k] = avgRank
		}
		i = j
	}

	// Sum ranks for group A
	rankSumA := 0.0
	for i, item := range combined {
		if item.group == 0 {
			rankSumA += ranks[i]
		}
	}

	// Calculate U statistic
	U1 := rankSumA - float64(n1*(n1+1))/2.0
	U2 := float64(n1*n2) - U1
	U := math.Min(U1, U2)

	// Normal approximation for p-value
	meanU := float64(n1*n2) / 2.0
	stdU := math.Sqrt(float64(n1*n2*(n1+n2+1)) / 12.0)

	if stdU == 0 {
		return 1.0
	}

	z := (U - meanU) / stdU

	// Two-tailed p-value (approximate using standard normal)
	pValue := 2.0 * normalCDF(-math.Abs(z))

	return pValue
}

// normalCDF approximates the standard normal cumulative distribution function
func normalCDF(z float64) float64 {
	// Approximation using error function
	return 0.5 * (1.0 + math.Erf(z/math.Sqrt2))
}

// CompareStats compares two Stats objects and returns comparison metrics
type Comparison struct {
	MedianDiffPct float64 // Percentage difference in medians
	PValue        float64 // Mann-Whitney U p-value
	HasOverlap    bool    // Whether ranges overlap
	Significant   bool    // Whether p < 0.05
}

// Compare performs statistical comparison between two groups
func Compare(statsA, statsB Stats) Comparison {
	medianDiff := 0.0
	if statsA.Median != 0 {
		medianDiff = ((statsB.Median - statsA.Median) / statsA.Median) * 100
	}

	pValue := MannWhitneyU(statsA.Values, statsB.Values)
	hasOverlap := HasOverlap(statsA, statsB)

	return Comparison{
		MedianDiffPct: medianDiff,
		PValue:        pValue,
		HasOverlap:    hasOverlap,
		Significant:   pValue < 0.05,
	}
}
