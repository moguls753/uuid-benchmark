# Paper evidence dashboard

The active GitHub Pages entry point is `docs/index.html`, with `assets/evidence.js`,
`assets/evidence.css` and `data/evidence.json`. It is a static, dependency-free
browser application (fonts have system fallbacks). Serve over HTTP, not `file://`.

## Audience and design

Paper readers and reviewers follow **finding → experiment → individual runs →
source evidence**. The existing editorial visual identity is retained: white and
pale-gray surfaces, serif headings, fine rules, stable key-scheme colors, and
monospaced measurement labels. Body text uses a readable system sans-serif.
No best-key badge, synthetic sparkline, cross-campaign scaling curve, or universal
engine ranking is presented.

- Findings: A5/A1/A2, PostgreSQL index structure, within-engine workload overview.
- Explorer: key-scheme, same-experiment scale and normalized engine comparisons.
- Data & methods: per-run table/CSV, source files with SHA-256, experiment register,
  sampling, measurement windows, statistics and limitations.
- State is encoded in the URL, including experiment, endpoint, scale, engine,
  comparison mode and normalization. Back/forward and old scenario links work.
- Graphical marks have scheme labels, text alternatives and exact-value tables.
  UUIDv4 additionally uses diamonds. Local table scrolling does not overflow the page.

## Selected sources — no automatic discovery

The dashboard includes exactly the paper's selected configurations:

| ID | Source | Scope |
|---|---|---|
| single-insert | `results/laptop/*_1conn_raw.csv`, explicit validated candidates | Four engines; 100K, 1M, 10M; throughput. PostgreSQL structure at 1M/10M. |
| single-read | Same historical collection | Four engines; 100K, 1M, 10M; throughput and process-window block-read rate. |
| single-update | Same historical collection | Four engines; 1M/10M; throughput. |
| single-ru | Selected rows of `*_1m_all_1conn_raw.csv` | 500K **preload**, not the archived RecordCount=1M metadata; throughput. |
| single-ih | `results/ih-corrected-consolidated-20261006T060522Z/` | 125 selected runs, five per engine/scheme; 100K preload; successful-operation throughput and per-run latency percentiles. |
| A1–A5 | Paper `data/nachlauf_a*.csv.runs.jsonl` | Precise primary run logs, not two-decimal raw CSV exports. A4 has n=3; the others n=5. |

Each series has `experiment`, `database`, `scale`, `metric`, `keyType`, `values`,
`runIds`, `median` and a source reference. Source files are downloadable snapshots;
`sourceDigest` hashes the sorted source manifest. A source CSV can contain archival,
unselected rows. The selection CSV downloaded by the UI contains **only the
selected plotted series**, including logical run IDs and source hashes.

The three selected IH replacements occupy their original block slots. They are
not added as extra observations. The failed consolidation, smoke tests, superseded
originals, historical IH, June cluster pilots and older concurrent single-node
measurements are not selected. They remain in the repository where available.

`assets/app.js`, its old modules, `data/data.json`, `data/annotations.json`, and
`scripts/convert_results.py` are legacy assets, **not loaded by the active site**.
Running the old converter does not rebuild the paper evidence dashboard.
Its old explanatory annotations must not be merged into the new UI.

## Build

Run from the benchmark repository with the companion paper checkout available:

```sh
python3 scripts/build_evidence.py --paper-root ../uuid-paper
python3 -m unittest discover -s scripts -p test_evidence.py
python3 scripts/check_evidence_package.py
python3 -m http.server 8000 --directory docs
```

The builder requires NumPy because it uses the paper's own statistical functions.
It imports the paper's validated loaders for historical inserts, structure, reads,
updates/RU and corrected IH; executes the cluster validity gate; recomputes the
UUIDv4/UUIDv7 contrasts using `gen_numbers.py`; and verifies rounded numerical
agreement with the paper's generated single-node and cluster macros, including
A2's I/O-exclusion inputs. All validation completes
before output files are written. The builder does not run a database benchmark,
modify measurement inputs, rewrite paper macros, commit or deploy.

The output bundles 700 selected metric series and the small source/analysis files.
The IH validator also checks archived selected-run evidence and full PostgreSQL
transaction logs **in the local source bundle**. Those large per-run logs and
build binaries are not copied to Pages. Thus the Pages snapshot supports numeric
traceability, not a claim that it contains the entire reproducibility bundle.
To rerun full validation, retain the selected IH bundle and the companion paper
checkout. Source snapshots under `data/sources/analysis/` preserve analysis code;
these copies are evidence, not a stand-alone replacement for that checkout.

Cluster manifests come from the paper's scrubbed copies and are sanitized again
for operational SSH flags and node addresses. Their output hashes refer to the
sanitized files. The precise run logs are copied byte-for-byte. Ten exact-path
`.gitignore` exceptions make only the selected dashboard snapshots eligible for
normal source tracking; raw operational manifests and logs remain ignored.

`check_evidence_package.py` builds and verifies a temporary local candidate from
the active assets and source files eligible under Git's normal tracked/addable
inventory. It stages no Git files and does not commit or deploy. To check a later
actual publication directory, run:

```sh
python3 scripts/check_evidence_package.py --artifact /path/to/publication-directory
```

This fails on a missing active asset, missing declared source or source hash
mismatch. The local candidate is a packaging rehearsal, not evidence of a deployed
GitHub Pages build.

## Statistical/display contract

- One point = one run; vertical ticks = medians. Vertical point offsets reveal
  overlap, never change x values or imply paired comparisons.
- Summary cluster and PostgreSQL panels have explicitly independent axes.
  Explorer multi-panel comparisons share an axis, inside one experiment only.
- Normalization divides each run by its **own configuration's** Sequential median.
  It is not a ratio of paired observations. Cross-engine view is throughput-only
  and normalized; structural proxies and absolute host speed are not compared.
- A1/A3 rank-sum tests are exact and one-sided; A2 is two-sided. Median-ratio
  intervals use 10,000 percentile-bootstrap resamples and seed 20260905.
  Four supporting endpoints have Bonferroni threshold 0.0125.
- A2/A5 Welch intervals refer to relative **mean** differences using the combined
  group mean as denominator. They are not median-ratio intervals and do not
  establish equivalence.
- Cluster read throughput counts attempts. No driver errors were recorded, but
  some reads returned no row; `failed=0` does not mean every lookup succeeded.
  Only corrected IH supports the all-success/all-hit claim.
- I/O per lookup is formed per run from the exported process-window rate divided
  by timed-loop throughput. Only the paper-retained A1/A3 endpoint is exposed.
  The arm-level exclusion fires when both median read I/O exceeds 1 block op/s
  and median per-run write/read I/O exceeds 5%. A2 meets both; these values are
  recomputed from logs and checked against the paper. Insert-side counters stop
  when the workload returns, missing later flushes/compactions by an unknown amount.
- PostgreSQL corrected-IH throughput uses the elapsed mixed phase, including
  pgbench startup, connection establishment and full transaction logging.
  Preload, target retrieval and validation are outside that window; validation
  can warm caches.
- A5 latency percentiles are deliberately not offered alongside per-lookup read
  latencies: the insert path includes batch timing. A5's supported endpoints here
  are throughput, table size and block I/O rates.
- Structural metrics are descriptive; hypotheses about timestamp wraps, SSTable
  overlap, compression and Bloom filters are not labeled proven mechanisms.
- Source hashes, not a wall-clock build timestamp, identify the selected snapshot.

## Browser validation

With Python Playwright and Chromium installed:

```sh
python3 scripts/check_evidence_browser.py
# Optional: one batched screenshot set for independent review
python3 scripts/check_evidence_browser.py --screenshots /tmp/uuid-evidence-review
```

The script serves the Git-eligible candidate, not the unrestricted working tree.
It requests all 49 declared sources over HTTP and checks their hashes, then checks
deep links, reload, same-view hash changes, history, filter focus, campaign
isolation, withheld endpoints, per-selection CSV contents, load failure and page
overflow at 1440/1024/390/320 pixels. Keyboard activation of the skip link must
preserve the Explorer/Data experiment, metric, URL and content. It does not deploy
or install dependencies.
