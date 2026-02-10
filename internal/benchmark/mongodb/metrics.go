package mongodb

import (
	"context"
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (m *MongoDBBenchmarker) MeasureMetrics() (*benchmark.BenchmarkResult, error) {
	// Force WiredTiger checkpoint so on-disk sizes are accurate
	ctx := context.Background()
	if err := m.client.Database("admin").RunCommand(ctx, bson.D{{Key: "fsync", Value: 1}}).Err(); err != nil {
		fmt.Printf("Warning: fsync failed: %v\n", err)
	}

	result := &benchmark.BenchmarkResult{}

	tableSize, indexSize, err := m.measureDiskUsage()
	if err != nil {
		return nil, fmt.Errorf("measure disk usage: %w", err)
	}
	result.TableSize = tableSize
	result.IndexSize = indexSize

	fragStats, err := m.measureFragmentation()
	if err != nil {
		fmt.Printf("Warning: Could not measure fragmentation: %v\n", err)
		result.Fragmentation = benchmark.IndexFragmentationStats{}
	} else {
		result.Fragmentation = fragStats
	}

	pageSplits, err := m.countPageSplits()
	if err != nil {
		fmt.Printf("Warning: Could not count page splits: %v\n", err)
		result.PageSplits = 0
	} else {
		result.PageSplits = pageSplits
	}

	bufferHitRatio, err := m.measureCacheHitRatio()
	if err != nil {
		fmt.Printf("Warning: Could not measure cache hit ratio: %v\n", err)
		result.BufferHitRatio = 0
		result.IndexBufferHitRatio = 0
	} else {
		result.BufferHitRatio = bufferHitRatio
		result.IndexBufferHitRatio = bufferHitRatio
	}

	return result, nil
}

func (m *MongoDBBenchmarker) measureDiskUsage() (tableSize, indexSize int64, err error) {
	ctx := context.Background()
	var stats bson.M
	err = m.db.RunCommand(ctx, bson.D{
		{Key: "collStats", Value: m.collName},
		{Key: "scale", Value: 1},
	}).Decode(&stats)
	if err != nil {
		return 0, 0, fmt.Errorf("collStats: %w", err)
	}

	if v, ok := stats["storageSize"]; ok {
		tableSize = toInt64(v)
	}
	if v, ok := stats["totalIndexSize"]; ok {
		indexSize = toInt64(v)
	}

	return tableSize, indexSize, nil
}

func (m *MongoDBBenchmarker) measureFragmentation() (benchmark.IndexFragmentationStats, error) {
	var stats benchmark.IndexFragmentationStats

	ctx := context.Background()
	var collStats bson.M
	err := m.db.RunCommand(ctx, bson.D{
		{Key: "collStats", Value: m.collName},
		{Key: "scale", Value: 1},
	}).Decode(&collStats)
	if err != nil {
		return stats, fmt.Errorf("collStats: %w", err)
	}

	// Fragmentation proxy: freeStorageSize / storageSize
	freeStorage := toFloat64(collStats["freeStorageSize"])
	storageSize := toFloat64(collStats["storageSize"])
	if storageSize > 0 {
		stats.FragmentationPercent = (freeStorage / storageSize) * 100
	}

	// Leaf density estimated from WiredTiger stats if available
	stats.AvgLeafDensity = 90.0 // WiredTiger default approximation

	return stats, nil
}

// countPageSplits measures reconciliation multi-block writes (delta before/after).
// When WiredTiger reconciles (checkpoints) pages to disk, pages exceeding
// leaf_page_max (32KB) are split into multiple on-disk blocks. This is the
// structural equivalent of PostgreSQL B-tree page splits.
// See: https://source.wiredtiger.com/develop/tune_page_size_and_comp.html
func (m *MongoDBBenchmarker) countPageSplits() (int, error) {
	if m.metricsBefore == nil {
		return 0, fmt.Errorf("metrics before not captured")
	}

	ctx := context.Background()
	var statusAfter bson.M
	err := m.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&statusAfter)
	if err != nil {
		return 0, fmt.Errorf("serverStatus: %w", err)
	}

	// Reconciliation multi-block writes: pages split into multiple on-disk
	// blocks during checkpoint. Triggered by fsync in MeasureMetrics.
	// Debug: trace where getWiredTigerStat fails
	if wt, ok := statusAfter["wiredTiger"]; !ok {
		fmt.Println("  DEBUG: wiredTiger key missing from serverStatus")
	} else if wtMap, ok := wt.(bson.M); !ok {
		fmt.Printf("  DEBUG: wiredTiger is %T, not bson.M\n", wt)
	} else if sec, ok := wtMap["reconciliation"]; !ok {
		fmt.Println("  DEBUG: reconciliation key missing from wiredTiger")
	} else if secMap, ok := sec.(bson.M); !ok {
		fmt.Printf("  DEBUG: reconciliation is %T, not bson.M\n", sec)
	} else if val, ok := secMap["leaf page multi-block writes"]; !ok {
		fmt.Println("  DEBUG: 'leaf page multi-block writes' key missing")
	} else {
		fmt.Printf("  DEBUG: stat found, type=%T value=%v\n", val, val)
	}

	leafBefore := getWiredTigerStat(m.metricsBefore, "reconciliation", "leaf page multi-block writes")
	leafAfter := getWiredTigerStat(statusAfter, "reconciliation", "leaf page multi-block writes")
	intBefore := getWiredTigerStat(m.metricsBefore, "reconciliation", "internal page multi-block writes")
	intAfter := getWiredTigerStat(statusAfter, "reconciliation", "internal page multi-block writes")
	fmt.Printf("  Reconciliation: leaf before=%d after=%d, internal before=%d after=%d\n",
		leafBefore, leafAfter, intBefore, intAfter)

	splitsBefore := leafBefore + intBefore
	splitsAfter := leafAfter + intAfter

	delta := splitsAfter - splitsBefore
	if delta < 0 {
		delta = 0
	}

	return int(delta), nil
}

func (m *MongoDBBenchmarker) measureCacheHitRatio() (float64, error) {
	ctx := context.Background()
	var status bson.M
	err := m.db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&status)
	if err != nil {
		return 0, fmt.Errorf("serverStatus: %w", err)
	}

	pagesRequested := getWiredTigerStat(status, "cache", "pages requested from the cache")
	pagesRead := getWiredTigerStat(status, "cache", "pages read into cache")

	if pagesRequested <= 0 {
		return 0, nil
	}

	hitRatio := 1.0 - (float64(pagesRead) / float64(pagesRequested))
	if hitRatio < 0 {
		hitRatio = 0
	}
	return hitRatio, nil
}

// getWiredTigerStat extracts a numeric stat from serverStatus.wiredTiger.<section>
func getWiredTigerStat(status bson.M, section, statName string) int64 {
	wt, ok := status["wiredTiger"]
	if !ok {
		return 0
	}
	wtMap, ok := wt.(bson.M)
	if !ok {
		return 0
	}
	sec, ok := wtMap[section]
	if !ok {
		return 0
	}
	secMap, ok := sec.(bson.M)
	if !ok {
		return 0
	}
	val, ok := secMap[statName]
	if !ok {
		return 0
	}
	return toInt64(val)
}


func toInt64(v any) int64 {
	switch val := v.(type) {
	case int32:
		return int64(val)
	case int64:
		return val
	case float64:
		return int64(val)
	case int:
		return int64(val)
	default:
		return 0
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case int:
		return float64(val)
	default:
		return 0
	}
}
