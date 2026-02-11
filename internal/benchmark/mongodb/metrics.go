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
		result.Fragmentation = benchmark.IndexFragmentationStats{
			AvgLeafDensity: -1,
			LeafPages:      -1,
			EmptyPages:     -1,
		}
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

	// MongoDB 8 / WiredTiger 12 does not expose leaf page counts or fill factor.
	// The btree stats (row-store leaf pages, number of key/value pairs) require
	// a tree_walk flag on the WiredTiger statistics cursor, which MongoDB never
	// triggers through its command API. Only PostgreSQL's pgstatindex() provides
	// real leaf density metrics.
	stats.AvgLeafDensity = -1 // N/A — tree_walk stats not triggered by MongoDB
	stats.LeafPages = -1      // N/A — same reason (tree_walk required)
	stats.EmptyPages = -1     // N/A — not exposed by MongoDB

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
	splitsBefore := getWiredTigerStat(m.metricsBefore, "reconciliation", "leaf page multi-block writes") +
		getWiredTigerStat(m.metricsBefore, "reconciliation", "internal page multi-block writes")

	splitsAfter := getWiredTigerStat(statusAfter, "reconciliation", "leaf page multi-block writes") +
		getWiredTigerStat(statusAfter, "reconciliation", "internal page multi-block writes")

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
	secVal := bsonLookup(wt, section)
	if secVal == nil {
		return 0
	}
	val := bsonLookup(secVal, statName)
	if val == nil {
		return 0
	}
	return toInt64(val)
}

// bsonLookup finds a key in a bson.M or bson.D value.
// The MongoDB Go driver v2 decodes nested documents as bson.D (ordered),
// not bson.M (map), so we must handle both types.
func bsonLookup(doc any, key string) any {
	switch d := doc.(type) {
	case bson.M:
		return d[key]
	case bson.D:
		for _, elem := range d {
			if elem.Key == key {
				return elem.Value
			}
		}
	}
	return nil
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
