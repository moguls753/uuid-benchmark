# Consolidated corrected IH data — paper-agent entry point

## Use these files

- **`runs.csv`**: authoritative selected raw view, 125 rows (122 originals + 3 repeats).
- **`summary.csv`**: 25 engine/scheme groups, each n=5; median and percent of the selected engine's Sequential median.
- **`selection.json`**: exact row-by-row source identity and replacement reasons.
- **`runs/<run-id>/`**: complete, unchanged selected per-run evidence, including PostgreSQL scripts/logs and validity exports.
- **`manifest.json`**: consolidation protocol and scope.

The three selected repeats are PostgreSQL block 5 monotonic ULID, PostgreSQL block 5 UUIDv4 and MySQL block 1 ULID. They were requested because recorded paper-build/render command windows overlapped the original mixed phases, not because their throughput was unfavorable. They preserve the original parameters, seeds, blocks, binary contents and image IDs. `stage=repeat` remains explicit in the raw CSV and copied evidence. The summary counts each selected repeat in its original analytical slot, exactly once.

## Archive only — do not include in the analysis

- `superseded-not-for-analysis/`: the three replaced original runs, retained for transparent auditing. These are NOT additional n=5 observations.
- `provenance/`: original campaign manifests, logs, source snapshots and binaries. Source campaign counts/stages describe those historical campaigns, not this analytical view.
- `tools/`: exact consolidation/validation code used to create this directory.

## Scope and limitations

All selected rows passed operation, read-hit, cardinality, phase/lifecycle timing and archived-evidence checks. PostgreSQL transaction logs were reread and reconciled. The raw and summary CSVs were read back. Full-size pilots were explicitly skipped in the original campaign. This is a documented consolidated view from two campaigns, not a new uninterrupted run or proof of an entirely interference-free host. Do not claim that throughput changes between original and repeat establish the causal effect of the paper builds.

No original source file was modified. No paper macros, text or figures were changed. No historical scenario or baseline is mixed into the selected results. Use the selected Sequential baseline, not the old IH baseline. Replacing paper values remains an explicit downstream step.
