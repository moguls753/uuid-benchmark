# Results dashboard

[Open dashboard](https://moguls753.github.io/uuid-benchmark/) · [Project README](../README.md)

The dashboard presents selected benchmark results, individual runs and
downloadable source data. It does not include every result in the repository;
new benchmark runs are not added automatically.

## Data

| Measurements | Scope |
|---|---|
| Single-node insert/read | PostgreSQL, MySQL, MongoDB and Cassandra; 100K–10M rows |
| Single-node update | Four engines; 1M/10M rows |
| Read/update mix | Four engines; 500K preload |
| Insert-heavy | Validated dataset; 100K preload, five runs per engine/key scheme |
| Cassandra A1–A5 | Cluster and single-node comparisons; 50M rows |

## Reading the results

- Points represent individual runs; ticks mark medians.
- Normalized values are relative to the Sequential median for the same configuration.
- Compare results within their experiment: hardware, replication, workload and
  measurement windows differ. Fragmentation and cache metrics are engine-specific.
- Cluster read throughput counts attempted lookups, including reads returning no row.
  The insert-heavy dataset separately verifies successful operations and read hits.

Source downloads include hashes for traceability. The selection CSV contains
only the plotted data; source files can also contain unselected archival rows.
Published files do not include the complete raw campaign archives. The copied
IH source README describes that full archive; its `runs/`, `provenance/` and
`tools/` directories are not part of the dashboard download.

Definitions and limitations: [Metrics methodology](METRICS_METHODOLOGY.md) ·
[Measurement notes](paper-notes.md).
