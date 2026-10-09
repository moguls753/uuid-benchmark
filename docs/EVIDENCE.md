# Results dashboard

**[Open dashboard](https://moguls753.github.io/uuid-benchmark/)** · [Project README](../README.md)

Explore selected benchmark results across PostgreSQL, MySQL, MongoDB and
Cassandra: findings, comparisons, individual runs and downloadable source data.
The dashboard shows a curated research dataset, not every result in the repository.
New benchmark runs do **not** appear automatically.

## View locally

From the repository root:

```sh
python3 -m http.server 8000 --directory docs
```

Open **http://localhost:8000**. The committed snapshot is ready to serve—no build,
Python packages or companion repository needed. Use HTTP, not `file://`.

The active files are `docs/index.html`, `docs/assets/evidence.js`,
`docs/assets/evidence.css` and `docs/data/evidence.json`. The application uses no
JavaScript framework; externally hosted fonts have system fallbacks. Older
`app.js`, `data.json`, `annotations.json` and `scripts/convert_results.py` are
legacy assets and do not power this dashboard.

## Included data

| Experiments | Coverage |
|---|---|
| Single-node insert/read | Four engines, 100K–10M rows; PostgreSQL index structure at 1M/10M |
| Single-node update | Four engines, 1M/10M rows |
| Read/update mix | 500K preload; archived RecordCount=1M is command-line metadata |
| Corrected insert-heavy | 100K preload; 125 selected runs, five per engine/key scheme |
| Cassandra A1–A5 | Selected cluster and single-node comparison arms at 50M rows; A4 has three repetitions, the others five |

The current snapshot contains 700 metric series and 49 source files. Downloads
include selected runs and source hashes. Source CSVs may also contain unselected
archival rows; the UI's selection CSV contains only the plotted selection.
Three insert-heavy replacement runs occupy their original slots, not additional
observations. Historical insert-heavy runs, pilots and superseded runs are excluded.

## Rebuild the dataset

Rebuilding is **not required to view the dashboard**. It requires Python with
NumPy, the companion paper checkout, and the selected corrected-IH campaign
archive. From the repository root:

```sh
python3 -m pip install numpy
python3 scripts/build_evidence.py --paper-root ../uuid-paper \
    --ih-results /path/to/ih-corrected-consolidated-20261006T060522Z
```

Without `--ih-results`, the builder looks in `results/`, then the sibling
`../benchmark-results-archiv/`. Inputs are explicitly selected, not discovered
from arbitrary CSVs. It uses the paper's analysis code, checks run validity and
numerical agreement, then writes the dashboard snapshot and source copies.
It does not execute benchmarks, change original measurements, commit or deploy.

Cluster manifest copies are scrubbed for SSH usernames, key paths and node
addresses; hashes identify those sanitized copies. Run logs are copied unchanged.
The published sources support numerical traceability, but do not include the
full campaign archives, binaries or PostgreSQL transaction logs. Full rebuilds
and validation therefore need the external inputs above.

## Validate changes

```sh
python3 -m unittest discover -s scripts -p test_evidence.py
python3 scripts/check_evidence_package.py

# Optional browser checks: requires Python Playwright and Chromium.
python3 scripts/check_evidence_browser.py
```

The package check verifies active assets and source hashes in a temporary
Git-eligible publication candidate. To check an existing publication directory:

```sh
python3 scripts/check_evidence_package.py --artifact /path/to/publication-directory
```

Browser checks cover source downloads, navigation, filters, CSV exports, error
states and responsive layout. Use `--screenshots /tmp/uuid-evidence-review` to
save screenshots. These checks do not deploy or prove the live site is current.

## Interpretation limits

- Points represent runs; ticks mark medians. Vertical offsets only separate
  overlapping points. Summary panels can have independent axes; Explorer
  comparisons share axes within one experiment.
- Normalized values divide each run by its own configuration's Sequential
  median, not a paired observation. Cross-engine comparisons are normalized
  throughput only; they do not establish a universal engine ranking.
- A1/A3 rank-sum tests are exact and one-sided; A2 is two-sided. Median-ratio
  intervals use 10,000 percentile-bootstrap resamples (seed 20260905).
  Four supporting endpoints use a Bonferroni threshold of 0.0125.
  A2/A5 Welch intervals instead describe relative **mean** differences, using
  the combined group mean as denominator; they do not establish equivalence.
- Cluster read throughput counts attempts, including reads returning no row.
  Zero driver errors does not mean all lookups succeeded. The corrected IH
  dataset validates successful operations and read hits; PostgreSQL IH timing
  includes pgbench startup, connections and transaction logging, but excludes
  preload and validation. Validation can warm caches.
- I/O per lookup divides each run's process-window rate by timed-loop throughput.
  Only the retained A1/A3 endpoint is exposed. A2 is excluded because both median
  read I/O exceeds 1 block op/s and median write/read I/O exceeds 5%.
  Insert counters omit later flushes/compactions by an unknown amount.
- A5 offers throughput, table size and block I/O rates, not insert batch latency
  alongside per-lookup read latency. Structural metrics are descriptive, not
  proof of a particular storage-engine mechanism.

See [metrics methodology](METRICS_METHODOLOGY.md) and
[paper notes](paper-notes.md) for further context.
