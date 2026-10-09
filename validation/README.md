# go-ycsb comparison

These scripts compare PostgreSQL throughput and latency for sequential integer
keys. Both clients run inside their respective database containers. This is a
baseline comparison, not validation of every key type or internal database metric.

## Run

Build `uuid-benchmark` as described in the [README](../README.md). Supply a
container-compatible `go-ycsb` binary with PostgreSQL support; the default path
is `../go-ycsb/bin/go-ycsb` relative to the repository root. The scripts also
require Bash, Docker, GNU grep and `bc`.

```bash
# From the repository root; omit YCSB_BIN to use the default path.
YCSB_BIN=/path/to/go-ycsb bash validation/run-comparison.sh insert 10000
```

Supported scenarios are `insert` and `read`; the default dataset is 10,000 rows.
Both use ten clients; the insert comparison uses batch size 1. The benchmark
still runs all key types, but the comparison script selects the sequential baseline.
Outputs are saved under `validation/results/`.

## Limitations

- Client implementations, schemas and database configurations differ; in-container
  execution does not make the workloads identical.
- `compare-results.sh` selects the latest files by timestamp, not matching run IDs.
  Verify that the selected files refer to the intended scenario and configuration.
- Its final success message is unconditional; inspect the actual differences and
  raw outputs. Console parsing can break when output labels or units change.
- Similar baseline throughput does not validate UUID generators, tail latency,
  page-split counts, fragmentation or cache metrics.

The [recorded comparison](SUPERVISOR_MEETING.md) summarizes one historical run.
Use dedicated benchmark resources: these scripts start and remove database
containers. See [Safety](../README.md#safety).
