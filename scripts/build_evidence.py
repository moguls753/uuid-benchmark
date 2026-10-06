#!/usr/bin/env python3
"""Build the paper evidence dashboard from explicitly selected, validated inputs.

python3 scripts/build_evidence.py --paper-root ../uuid-paper
Requires numpy (the paper's analysis dependency). No benchmark is executed.
Historical data.json remains archival; the active site reads evidence.json only.
"""
import argparse
import csv
import hashlib
import importlib
import json
import math
from pathlib import Path
import re
import statistics as st
import sys

ROOT = Path(__file__).resolve().parent.parent
IH = 'ih-corrected-consolidated-20261006T060522Z'
ENGINES = ['postgres', 'mysql', 'mongodb', 'cassandra']
KEYS = ['SEQUENTIAL', 'UUIDV1', 'UUIDV7', 'ULID', 'ULID_MONOTONIC', 'UUIDV4']
ARMS = {
    'A1': 'nachlauf_a1_read_50m_4g_n5',
    'A2': 'nachlauf_a2_read_50m_32g_n5',
    'A3': 'nachlauf_a3_read_50m_4g_single_n5',
    'A4': 'nachlauf_a4_bridge_head_50m_4g_n3',
    'A5': 'nachlauf_a5_insert_50m_4g_n5',
}
METRICS = {
    'throughput': {'label': 'Throughput', 'unit': 'ops/s', 'note': 'Throughput counts attempted operations in historical workloads and A1–A5; corrected IH counts completed successful operations. A1–A4 recorded no driver errors, but some lookups returned no row. LOCAL_ONE does not guarantee read-after-write consistency with RF3. Inserts include identifier generation.'},
    'p50_latency_us': {'label': 'Latency · p50', 'unit': 'µs', 'note': 'Per-run median operation latency. The chart shows the distribution of these per-run percentiles, not a pooled latency distribution.'},
    'p95_latency_us': {'label': 'Latency · p95', 'unit': 'µs', 'note': 'Per-run 95th-percentile operation latency; not the 95th percentile of the plotted run values.'},
    'p99_latency_us': {'label': 'Latency · p99', 'unit': 'µs', 'note': 'Per-run 99th-percentile operation latency. A2 runs occupy two latency ranges; these are descriptive observations, not evidence of a mechanism.'},
    'read_iops': {'label': 'Block-read rate', 'unit': 'block ops/s', 'note': 'cgroup v2 process-window rate, including setup and background activity, not just the timed query loop. Exported historical zeroes are limited by two-decimal precision.'},
    'write_iops': {'label': 'Block-write rate', 'unit': 'block ops/s', 'note': 'cgroup v2 process-window rate. During reads this includes concurrent background work. Insert-side counters are read when the workload returns: later flushes and compactions are omitted, under-counting insert-side writes by an unknown amount.'},
    'io_per_op': {'label': 'Block I/O per lookup', 'unit': 'block ops/lookup', 'note': 'Per-run read_iops / read_throughput, following the paper protocol. Different measurement windows: not an isolated disk-access count per query. Excluded when both the arm-wide median read I/O exceeds 1 block operation/s and the median per-run write/read I/O ratio exceeds 5%. A2 meets both conditions; A4 is not offered.'},
    'table_size_mb': {'label': 'Table size', 'unit': 'MiB', 'note': 'Observed Cassandra live table size, summed across all nodes. Every row reuses the same 1 KiB payload. Percent differences are specific to these highly compressible data; not a pure key-byte cost.'},
    'index_size_mb': {'label': 'Index size', 'unit': 'MiB', 'note': 'PostgreSQL primary-index size after loading. The historical field ends in _mb but contains MiB.'},
    'page_splits': {'label': 'B-tree split records', 'unit': 'records', 'note': 'PostgreSQL instance-wide WAL B-tree split records around the insert workload. Split counts are not interchangeable with page occupancy or fragmentation.'},
    'avg_leaf_density': {'label': 'Leaf density', 'unit': '%', 'note': 'Occupied percentage of usable PostgreSQL primary-index leaf-page space after loading.'},
    'fragmentation': {'label': 'Leaf fragmentation', 'unit': '%', 'note': 'PostgreSQL pgstatindex physical leaf-page ordering metric. Not percentage wasted space and not comparable to other engines’ fragmentation proxies.'},
    'sstable_count': {'label': 'SSTables at read start', 'unit': 'files', 'note': 'Cassandra SSTable snapshot at read start, summed across nodes. Count alone does not identify range overlap or read amplification.'},
    'bloom_filter_fp': {'label': 'Bloom false-positive delta', 'unit': 'count', 'note': 'Counter change clamped to zero per node, then summed. Compaction can mask increases. Zero does not mean no filter was consulted. Filters test partitions, not individual clustering keys; this is descriptive, not a mechanism test.'},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(payload):
    return hashlib.sha256(payload).hexdigest()


def build(paper, output):
    sys.path.insert(0, str(paper / 'scripts'))
    inserts = importlib.import_module('part_one_insert')
    reads = importlib.import_module('part_one_reads')
    updates = importlib.import_module('part_one_updates')
    structure = importlib.import_module('pg_insert_structure')
    ih = importlib.import_module('part_one_ih')
    gn = importlib.import_module('gen_numbers')
    gate = importlib.import_module('check_validity')
    laptop = ROOT / 'results/laptop'
    selected = ROOT / 'results' / IH
    # These gates reject ambiguous sources, incomplete groups and invalid evidence.
    ins = inserts.load_inserts(laptop)
    rd = reads.load_reads(laptop)
    upd = updates.load_updates(laptop)
    pg = structure.load_structure(laptop)
    ih_data = ih.load_ih(selected)
    single_macros = dict(re.findall(r'\\newcommand\{\\Num(\w+)\}\{([^}]+)\}', (paper/'sections/numbers-single.tex').read_text()))
    for name, value in reads.read_macros(rd) + updates.update_macros(upd) + ih.ih_macros(ih_data):
        require(single_macros.get(name) == value, f'Single-node paper macro mismatch: {name}')
    for engine, word in [('postgres', 'Postgres'), ('mysql', 'Mysql'), ('mongodb', 'Mongo'), ('cassandra', 'Cassandra')]:
        ratio, _, _ = inserts.normalized(ins[engine, '10m'], 'UUIDV4')
        name = f'SingleInsert{word}VFourDeficitTenM'
        require(single_macros.get(name) == f'{100*(1-ratio):.1f}', f'Insert paper macro mismatch: {name}')
    for key, suffix in [('UUIDV1', 'VOne'), ('UUIDV7', 'VSeven')]:
        require(single_macros.get('SinglePgTenMSplits'+suffix) == f"{st.median(pg['10m']['page_splits'][key]):,.0f}", f'Structure macro mismatch: {key}')
        require(single_macros.get('SinglePgTenMDensity'+suffix) == f"{st.median(pg['10m']['avg_leaf_density'][key]):.2f}", f'Density macro mismatch: {key}')
    if gate.main() != 0:
        raise ValueError('Cluster validity gate failed')
    files, sources, entries = {}, {}, []

    def source(path, name):
        content = path.read_bytes()
        if name in files and files[name] != content:
            raise ValueError(f'Conflicting source {name}')
        files[name] = content
        sources[name] = {'path': 'data/sources/' + name, 'sha256': digest(content), 'bytes': len(content)}
        return name

    def add(exp, engine, scale, metric, groups, src, ids=None):
        for key, values in groups.items():
            if not values or not all(math.isfinite(v) and v >= 0 for v in values):
                raise ValueError(f'Invalid values: {exp}/{engine}/{key}/{metric}')
            entries.append(dict(experiment=exp, database=engine, scale=scale, metric=metric,
                                keyType=key, values=values, median=st.median(values),
                                runIds=ids[key] if ids else [str(i) for i in range(1, len(values)+1)],
                                source=src))

    def historical(engine, scale, tag):
        paths = [laptop / f'{engine}_{scale}_{t}_1conn_raw.csv' for t in (tag, 'all')]
        paths = [p for p in paths if p.exists()]
        if len(paths) != 1:
            raise ValueError(f'Ambiguous source: {paths}')
        return source(paths[0], 'single/' + paths[0].name)

    for (engine, scale), groups in ins.items():
        add('single-insert', engine, scale, 'throughput', groups, historical(engine, scale, 'insert-performance'))
    for (engine, scale), groups in rd.items():
        src = historical(engine, scale, 'read-performance')
        for metric, values in groups.items():
            add('single-read', engine, scale, 'throughput' if metric == 'read_throughput' else metric, values, src)
    for (engine, endpoint), groups in upd.items():
        ru = endpoint == 'read_update'
        scale = '1m' if ru else endpoint.split('_')[1]
        src = historical(engine, scale, 'mixed-read-update' if ru else 'update-performance')
        add('single-ru' if ru else 'single-update', engine, '500k' if ru else scale, 'throughput', groups, src)
    for scale, groups in pg.items():
        src = historical('postgres', scale, 'insert-performance')
        for metric, values in groups.items():
            add('single-insert', 'postgres', scale, metric, values, src)

    ih_source = source(selected / 'runs.csv', 'ih/runs.csv')
    for name in ('summary.csv', 'selection.json', 'manifest.json', 'complete.json', 'README.md'):
        source(selected / name, 'ih/' + name)
    with (selected / 'runs.csv').open(newline='') as handle:
        records = list(csv.DictReader(handle))
    for engine in ENGINES:
        keys = KEYS + (['OBJECTID'] if engine == 'mongodb' else [])
        rows = {k: sorted([r for r in records if r['engine'] == engine and r['scheme'].upper() == k], key=lambda r: int(r['block'])) for k in keys}
        ids = {k: [r['run_id'] for r in rr] for k, rr in rows.items()}
        for key, rr in rows.items():
            require([float(r['throughput']) for r in rr] == ih_data[engine][key.lower()], f'IH selected ordering differs: {engine}/{key}')
        for metric, field in [('throughput', 'throughput')] + [(f'p{p}_latency_us', f'latency_p{p}_us') for p in (50, 95, 99)]:
            add('single-ih', engine, '100k', metric, {k: [float(r[field]) for r in rr] for k, rr in rows.items()}, ih_source, ids)

    contrasts, io_contexts = {}, {}
    for arm, stem in ARMS.items():
        src = source(paper / 'data' / (stem + '.csv.runs.jsonl'), 'cluster/' + stem + '.csv.runs.jsonl')
        # Only the paper's scrubbed manifests are included; never copy raw SSH flags.
        meta = json.loads((paper / 'data' / (stem + '.csv.meta.json')).read_text())
        def scrub(value):
            if isinstance(value, dict):
                return {k: ('<scrubbed>' if k in ('ssh-user', 'ssh-key', 'nodes') else scrub(v)) for k, v in value.items()}
            if isinstance(value, list):
                return [scrub(v) for v in value]
            return value
        name = 'cluster/' + stem + '.csv.meta.json'
        content = (json.dumps(scrub(meta), indent=2) + '\n').encode()
        files[name] = content
        sources[name] = {'path': 'data/sources/' + name, 'sha256': digest(content), 'bytes': len(content), 'note': 'Sanitized manifest; operational SSH flags removed.'}
        recs = gn.nl_load(stem)
        n = 3 if arm == 'A4' else 5
        if arm != 'A5':
            require(all(r['metrics']['failed'] == 0 for r in recs), f'Driver-error disclosure requires updating: {arm}')
        require(len(recs) == 6 * n, f'Incomplete arm: {arm}')
        for key in KEYS:
            require(sorted(r['run'] for r in recs if r['key_type'].upper() == key) == list(range(1, n+1)), f'Invalid repetitions: {arm}/{key}')
        metric_map = {'throughput': 'throughput' if arm == 'A5' else 'read_throughput',
                      'table_size_mb': 'table_size_mb', 'read_iops': 'read_iops', 'write_iops': 'write_iops'}
        if arm != 'A5':
            metric_map.update({m: m for m in ['p50_latency_us', 'p95_latency_us', 'p99_latency_us', 'sstable_count', 'bloom_filter_fp']})
        series = {metric: gn.nl_runs(recs, field) for metric, field in metric_map.items()}
        if arm in ('A1', 'A2', 'A3'):
            rio = [r['metrics']['read_iops'] for r in recs]
            ratios = [r['metrics']['write_iops']/r['metrics']['read_iops'] if r['metrics']['read_iops'] else math.inf for r in recs]
            median_read, median_write_read = st.median(rio), st.median(ratios)
            require(math.isfinite(median_write_read), f'Infinite arm median requires a display policy: {arm}')
            io_contexts[arm] = dict(medianReadIops=median_read, medianWriteReadPct=100*median_write_read,
                                    excluded=median_read > 1 and median_write_read > .05)
        if arm in ('A1', 'A3'):
            require(not io_contexts[arm]['excluded'], f'I/O exclusion changed: {arm}')
            series['io_per_op'] = gn.nl_io_per_op(recs)
        if arm == 'A2':
            require(io_contexts[arm]['excluded'], 'A2 I/O exclusion changed')
        for metric, groups in series.items():
            add(arm, 'cassandra', '50m', metric, groups, src)
        contrasts[arm] = {}
        if arm in ('A1', 'A2', 'A3'):
            for metric in ['throughput', 'p50_latency_us', 'p95_latency_us', 'p99_latency_us'] + (['io_per_op'] if arm != 'A2' else []):
                a, b = series[metric]['UUIDV4'], series[metric]['UUIDV7']
                ci = gn.boot_ratio_ci(a, b, .95)
                alternative = 'two' if arm == 'A2' else ('less' if metric == 'throughput' else 'greater')
                contrasts[arm][metric] = {'ratio': st.median(a)/st.median(b), 'ci': ci,
                                         'p': gn.exact_pooled(a, b, alternative), 'sided': 'two-sided' if arm == 'A2' else 'one-sided'}
        if arm in ('A2', 'A5'):
            diff, lo, hi = gn.welch_ci95(series['throughput']['UUIDV4'], series['throughput']['UUIDV7'])
            contrasts[arm]['meanThroughput'] = {'difference': diff, 'ci': [lo, hi]}

    # Bind displayed primary results to the current paper's generated numbers.
    macros = dict(re.findall(r'\\newcommand\{\\Num(\w+)\}\{([^}]+)\}', (paper/'sections/numbers.tex').read_text()))
    for arm, word in [('A1','AOne'), ('A2','ATwo'), ('A3','AThree')]:
        io = io_contexts[arm]
        require(abs(float(macros['Nl'+word+'MedReadIops']) - io['medianReadIops']) <= .0051, f'I/O macro mismatch: {arm}')
        require(abs(float(macros['Nl'+word+'WriteReadPct']) - io['medianWriteReadPct']) <= .0051, f'Background I/O macro mismatch: {arm}')
        for metric, mw in [('throughput','Tput'), ('p50_latency_us','PFifty'), ('p95_latency_us','PNinetyFive'), ('p99_latency_us','PNinetyNine')] + ([('io_per_op','IoOp')] if arm != 'A2' else []):
            c = contrasts[arm][metric]
            for suffix, val in [('Ratio',c['ratio']), ('CiNinetyFiveLo',c['ci'][0]), ('CiNinetyFiveHi',c['ci'][1])]:
                require(abs(float(macros['Nl'+word+mw+suffix]) - val) <= .00051, f'Paper macro mismatch: {arm}/{metric}/{suffix}')
            require(abs(float(macros['Nl'+word+mw+'P']) - c['p']) <= .0000051, f'Paper p-value mismatch: {arm}/{metric}')
    for arm, word in [('A2', 'ATwo'), ('A5', 'AFive')]:
        c = contrasts[arm]['meanThroughput']
        for suffix, val in [('DiffPct', c['difference']), ('CiLoPct', c['ci'][0]), ('CiHiPct', c['ci'][1])]:
            require(abs(float(macros['Nl'+word+'EquivTputVSeven'+suffix])-val) <= .0051, f'Paper mean-contrast mismatch: {arm}/{suffix}')
    for name in ['gen_numbers.py', 'part_one_insert.py', 'part_one_reads.py', 'part_one_updates.py', 'pg_insert_structure.py', 'part_one_ih.py', 'check_validity.py']:
        source(paper / 'scripts' / name, 'analysis/' + name)
    source(paper/'sections/numbers.tex', 'analysis/numbers.tex')
    source(paper/'sections/numbers-single.tex', 'analysis/numbers-single.tex')

    experiments = [
        dict(id='single-insert', label='Single-node · Inserts', family='single', section='§5.1–5.2 · Figures 1–2', n=5, memory='8 GB', nodes=1, clients=1, rows='100K / 1M / 10M', sampler='No read targets', note='Attempted insert throughput includes key generation. Failure counts were not exported for these pure-insert runs. Structure endpoints are PostgreSQL-only, at 1M and 10M.'),
        dict(id='single-read', label='Single-node · Point lookups', family='single', section='§5.3 · Point lookups', n=5, memory='8 GB', nodes=1, clients=1, rows='100K / 1M / 10M', sampler='Engine-specific target selection', note='PostgreSQL draws random keys; MySQL fetches a limited unordered list, MongoDB natural-order IDs, Cassandra the smallest clustering keys. Identifier fetching is outside the timed loop but inside the broader I/O window where applicable. Compare within each engine.'),
        dict(id='single-update', label='Single-node · Updates', family='single', section='§5.4 · Update / mixed table', n=5, memory='8 GB', nodes=1, clients=1, rows='1M / 10M', sampler='Engine-specific target selection', note='Attempted-operation throughput. The insert ranking need not carry over to updates. Target selection differs between engines as in the historical read workloads.'),
        dict(id='single-ru', label='Single-node · Read / update mix', family='single', section='§5.4 · Update / mixed table', n=5, memory='8 GB', nodes=1, clients=1, rows='500K preload', sampler='Prepared target lists; see methods', note='50% read / 50% update selection probabilities; 200K requested operations. Attempted-operation throughput. The archived CSV RecordCount=1M is command-line metadata, NOT the 500K preload target.'),
        dict(id='single-ih', label='Single-node · Insert-heavy (corrected)', family='ih', section='§5.4 · Update / mixed table', n=5, memory='8 GB', nodes=1, clients=1, rows='100K preload', sampler='Uniform sampling from all preload IDs', note='70% insert / 30% read selection probabilities; 200K completed successful operations. Fixed pool of 100K preload IDs; new inserts never enter the pool. 125 selected runs: 122 originals + 3 documented replacements. No historical IH measurements or baselines are mixed in.'),
    ]
    for arm in ['A5', 'A1', 'A2', 'A3', 'A4']:
        mem = '32 GB' if arm == 'A2' else '4 GB'
        nodes = 1 if arm == 'A3' else 3
        note = 'Reads follow loading without waiting for compaction; one synchronous client; cgroup counters include background work. HDD storage, not SSD. '
        note += {'A1':'All UUIDv4 throughput runs fall below all UUIDv7 runs. This does not identify a causal mechanism.',
                 'A2':f"Container memory, heap and new generation all change together. Read I/O is near zero. I/O-per-lookup is excluded: arm-wide median read I/O is {io_contexts['A2']['medianReadIops']:.2f} block ops/s (>1), and the median per-run write/read ratio is {io_contexts['A2']['medianWriteReadPct']:.2f}% (>5%). No significant throughput difference is NOT evidence of equivalence.",
                 'A3':'Node count AND replication factor differ from A1 (1/RF1 versus 3/RF3); this is not an isolated node-count effect.',
                 'A4':'Method check only, n=3: the two smallest clustering keys per partition are fetched and read in returned order. This changes both target selection and order; fetching falls inside the I/O window.',
                 'A5':'The load is the measured insert phase: eight concurrent writers, attempted rows/s. Small batch losses are recorded (at most 3,800 / 50M rows). Repeated 1 KiB payload; table sizes are cluster sums and cannot be read as a general storage-overhead percentage.'}[arm]
        if arm == 'A5':
            note = note[note.index('The load'):]
        experiments.append(dict(id=arm, label=f'{arm} · {"Inserts" if arm == "A5" else "Reads"} · {mem} · {nodes} {"node" if nodes == 1 else "nodes"}' + (' · head sampler' if arm == 'A4' else ''), family='cluster', section='§7 · Sampler check' if arm == 'A4' else '§6 · Cluster results', n=3 if arm == 'A4' else 5, memory=mem, heap='8G / 2G' if arm == 'A2' else '2G / 512M', nodes=nodes, rf=nodes, clients=8 if arm == 'A5' else 1, rows='50M', sampler='Not applicable (insert)' if arm == 'A5' else ('Head of partition; returned order' if arm == 'A4' else 'Uniform insertion positions; shuffled order'), note=note))

    identity = [(e['experiment'], e['database'], e['scale'], e['metric'], e['keyType']) for e in entries]
    require(len(identity) == len(set(identity)), 'Duplicate series')
    for e in entries:
        exp = next(x for x in experiments if x['id'] == e['experiment'])
        require(len(e['values']) == exp['n'], f'Incomplete series: {e}')
        require(len(set(e['runIds'])) == exp['n'], f'Duplicate run identity: {e}')
    manifest_digest = digest(json.dumps(sources, sort_keys=True).encode())
    data = dict(schemaVersion=1, snapshot='Paper evidence · 6 October 2026',
                sourceDigest=manifest_digest, experiments=experiments, metrics=METRICS,
                entries=entries, contrasts=contrasts, ioContexts=io_contexts, sources=sources,
                selectedIH=IH, validation='Paper loaders + selected IH evidence validation + cluster validity gate + numerical agreement with paper contrast macros')
    # No site outputs are written until all inputs and comparisons have passed.
    output.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        dest = output/'sources'/name
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(content)
    (output/'evidence.json').write_text(json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
    print(f'Wrote {len(entries)} series and {len(sources)} source files to {output}; digest {manifest_digest[:12]}')
    return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--paper-root', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=ROOT/'docs/data')
    args = parser.parse_args()
    build(args.paper_root.resolve(), args.output.resolve())
