# UUID Benchmark

Compares sequential integer keys, UUIDv1, UUIDv4, UUIDv7, ULID and monotonic ULID across **PostgreSQL 18, MySQL 8, MongoDB 8 and Cassandra 5**. MongoDB also includes ObjectId. Measures throughput, latency, storage, cache behavior and I/O.

[Results dashboard](https://moguls753.github.io/uuid-benchmark/) · [Dashboard documentation](docs/EVIDENCE.md)

## Build and run

Requires Go 1.22+, Docker with Compose, and Linux with cgroup v2 for I/O metrics.
Allow at least 8 GB RAM plus host overhead and 20 GB free disk for small runs.
Initial setup downloads images and builds PostgreSQL extensions.

From the repository root:

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
./uuid-benchmark -database=postgres -scenario=insert-performance \
    -num-records=100000 -connections=1 -num-runs=5 -output=results.csv
```

Each scenario tests all supported key types in fresh database containers.
Use `-database=mysql`, `mongodb` or `cassandra` for the other engines.

| Option | Default | Purpose |
|---|---|---|
| `-scenario` | `insert-performance` | Workload to run |
| `-num-records` | `100000` | Dataset size |
| `-num-ops` | `10000` | Read/update/mixed operation count |
| `-connections` | `1` | Concurrent workers |
| `-batch-size` | `100` | Batch size |
| `-num-runs` | `1` | Repetitions per key type |
| `-output` | none | CSV results in multi-run mode and run metadata |

Scenarios: `insert-performance`, `read-performance`, `update-performance`,
`mixed-insert-heavy` (70% insert / 30% read), `mixed-read-update` (50% read /
50% update), or `all`. Full options: `./uuid-benchmark -help`.

## Cassandra clusters

PostgreSQL, MySQL and MongoDB run single-node. Cassandra supports:

| `-cluster-mode` | Deployment | Workload location |
|---|---|---|
| `local-single` (default) | One local container | Inside container |
| `local-cluster` | Three local containers; correctness checks only | Orchestrator |
| `remote-cluster` | Separate hosts managed over SSH | Orchestrator |

The local cluster exposes only one coordinator; do not use it for performance measurements.

```bash
./uuid-benchmark -database=cassandra -cluster-mode=remote-cluster \
    -nodes=host1:9042,host2:9042,host3:9042 \
    -ssh-user="$USER" -ssh-key="$HOME/.ssh/id_ed25519" \
    -scenario=insert-performance -num-records=1000000 -num-runs=3 \
    -output=cassandra-cluster.csv
```

Remote hosts need Docker access for the SSH user, image-registry access,
SSH/CQL connectivity from the orchestrator and internode connectivity (port 7000).

- Single-node defaults: RF1 / `local_one`. Cluster defaults: RF3 / `local_quorum`.
  Override with `-replication-factor` and `-consistency`; RF must not exceed the
  remote host count. One remote host requires `-single-node` (defaults to RF1).
- Remote resource defaults: `-cassandra-cpus=8`, `-cassandra-memory=32g`,
  `-cassandra-heap=8G`, `-cassandra-newgen=2G`. Adjust to host capacity.
- Use `-cassandra-image` to pin an image digest instead of the default `cassandra:5`.

## Results

Multi-run mode exports CSV summaries. With `-output`, `.meta.json` records
configuration and provenance; `.runs.jsonl` records completed runs.
Engine-specific metrics are described in [Metrics methodology](docs/METRICS_METHODOLOGY.md).
See [YCSB validation](validation/README.md) for comparison runs.

## Safety

Use dedicated, isolated or firewalled benchmark hosts. Database ports are
published on all interfaces with default credentials; Cassandra has no
authentication. Do not expose them to the public Internet.

Runs remove benchmark containers and volumes. Remote Cassandra replaces
containers named `cassandra` and volumes named `cassandra-data-<host>`.
SSH host-key verification is disabled; use only trusted private networks.
