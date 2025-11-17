# Thesis Package

This package will contain thesis-specific functionality for statistical validation and automated testing.

## Planned Features

### 1. Statistical Validation
- Run each scenario multiple times (5-10 runs recommended)
- Calculate median, mean, standard deviation
- Compute confidence intervals
- Export results with error bars

### 2. CSV/JSON Export
- Export raw benchmark results to CSV for plotting
- Generate JSON summaries for programmatic analysis
- Python/R plotting scripts

### 3. Scaling Analysis
- Automated testing with multiple dataset sizes (1M, 10M, 100M records)
- Analyze how fragmentation/performance scales
- Generate scaling comparison tables

### 4. Full Thesis Evaluation
- "thesis-full" scenario that runs all 6 scenarios with statistical validation
- Comprehensive CSV export for all metrics
- Summary statistics and analysis

## Usage (Planned)

```bash
# Run single scenario with statistical validation
./uuid-benchmark -scenario=insert-performance -runs=5 -output=results.csv

# Run all scenarios for thesis
./uuid-benchmark -scenario=thesis-full -runs=5 -output-dir=thesis-data/

# Scaling analysis
./uuid-benchmark -scenario=thesis-scaling -sizes=1M,10M,100M
```

## Status

**Not yet implemented** - This is a placeholder for future thesis functionality.
Current focus: Measurement correctness and multi-database support.
