#!/usr/bin/env python3
"""Generate numbers.tex from data/*.csv. Fail-closed.

Every statistic quoted in the paper comes from a macro in the generated
sections/numbers.tex. No hand-typed numbers. If any structural expectation
about the input data is violated, this script aborts with a non-zero exit
code instead of silently generating output (Cockpit workflow rule).

Statistics follow drafts/verifikation.md; the analysis choices (exact pooled
contrast, permutation KW with fixed seed, Welch 95% CI) are documented there.

Usage: python3 scripts/gen_numbers.py   (run from repo root)
Output: sections/numbers.tex, stdout check log.
"""

import csv
import itertools
import math
import statistics as st
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
OUT = ROOT / "sections" / "numbers.tex"

TYPES = ["SEQUENTIAL", "UUIDV1", "UUIDV7", "ULID", "ULID_MONOTONIC", "UUIDV4"]
ORDERED = TYPES[:5]  # time-ordered / sequential class
PERM_N = 200_000
PERM_SEED = 20260831

FAILURES: list[str] = []


def fail(msg: str) -> None:
    FAILURES.append(msg)
    print(f"FAIL: {msg}")


def check(cond: bool, msg: str) -> None:
    if not cond:
        fail(msg)


# ---------------------------------------------------------------- loading

def load_raw(name: str) -> list[dict]:
    path = DATA / name
    if not path.exists():
        fail(f"missing input file {path}")
        return []
    with open(path) as fh:
        rows = list(csv.DictReader(fh))
    check(len(rows) > 0, f"{name}: empty")
    return rows


def runs(rows: list[dict], scen: str, met: str) -> dict[str, list[float]]:
    d: dict[str, list[float]] = {}
    for r in rows:
        if r["Scenario"] == scen and r["Metric"] == met:
            d[r["KeyType"]] = [float(r["Run1"]), float(r["Run2"]), float(r["Run3"])]
    return d


def expect_full(d: dict, name: str) -> bool:
    ok = set(d.keys()) == set(TYPES) and all(len(v) == 3 for v in d.values())
    check(ok, f"{name}: expected 6 key types x 3 runs, got "
              f"{sorted(d.keys())} with lengths {[len(v) for v in d.values()]}")
    return ok


# ---------------------------------------------------------------- statistics

def ranks(vals: list[float]) -> list[float]:
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    r = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2 + 1
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def exact_pooled(g1: list[float], rest: list[float], alt: str) -> float:
    """Exact rank-sum test for len(g1) vs rest by full enumeration."""
    all_ = list(g1) + list(rest)
    rk = ranks(all_)
    obs = sum(rk[: len(g1)])
    lo = hi = tot = 0
    for comb in itertools.combinations(range(len(all_)), len(g1)):
        s = sum(rk[i] for i in comb)
        tot += 1
        if s <= obs + 1e-9:
            lo += 1
        if s >= obs - 1e-9:
            hi += 1
    return {"less": lo / tot, "greater": hi / tot,
            "two": min(1.0, 2 * min(lo, hi) / tot)}[alt]


def cliffs(g1: list[float], g2: list[float]) -> float:
    gt = sum(1 for a in g1 for b in g2 if a > b)
    lt = sum(1 for a in g1 for b in g2 if a < b)
    return (gt - lt) / (len(g1) * len(g2))


def kw_h(groups: list[np.ndarray]) -> float:
    all_ = np.concatenate(groups)
    n = len(all_)
    order = np.argsort(all_, kind="mergesort")
    rk = np.empty(n)
    sorted_vals = all_[order]
    i = 0
    while i < n:
        j = i
        while j + 1 < n and sorted_vals[j + 1] == sorted_vals[i]:
            j += 1
        rk[order[i:j + 1]] = (i + j) / 2 + 1
        i = j + 1
    h = 0.0
    idx = 0
    for g in groups:
        m = len(g)
        h += rk[idx:idx + m].sum() ** 2 / m
        idx += m
    h = 12.0 / (n * (n + 1)) * h - 3 * (n + 1)
    _, counts = np.unique(all_, return_counts=True)
    corr = 1 - (counts ** 3 - counts).sum() / (n ** 3 - n)
    return h / corr if corr > 0 else float("nan")


def kw_perm(groups_in: list[list[float]], seed_offset: int = 0) -> tuple[float, float]:
    groups = [np.asarray(g, dtype=float) for g in groups_in]
    h0 = kw_h(groups)
    rng = np.random.default_rng(PERM_SEED + seed_offset)
    all_ = np.concatenate(groups)
    sizes = [len(g) for g in groups]
    cnt = 0
    for _ in range(PERM_N):
        rng.shuffle(all_)
        gs = []
        idx = 0
        for m in sizes:
            gs.append(all_[idx:idx + m])
            idx += m
        if kw_h(gs) >= h0 - 1e-12:
            cnt += 1
    return h0, (cnt + 1) / (PERM_N + 1)


def welch_ci95(a: list[float], b: list[float]) -> tuple[float, float, float]:
    """Welch 95% CI for mean(a)-mean(b), in percent of the mean over all
    values of both groups (protocol 9, reading declared in the number sheet)."""
    ma, mb = st.mean(a), st.mean(b)
    va, vb = st.variance(a), st.variance(b)
    na, nb = len(a), len(b)
    se = math.sqrt(va / na + vb / nb)
    df = (va / na + vb / nb) ** 2 / ((va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1))
    # t quantile via numpy-free bisection on the CDF (regularized incomplete beta)
    from math import lgamma

    def betacf(x: float, p: float, q: float) -> float:
        c, d = 1.0, 1.0 - (p + q) * x / (p + 1)
        if abs(d) < 1e-300:
            d = 1e-300
        d = 1 / d
        h = d
        for m in range(1, 200):
            m2 = 2 * m
            aa = m * (q - m) * x / ((p + m2 - 1) * (p + m2))
            d = 1 + aa * d
            c = 1 + aa / c
            d = 1 / (d if abs(d) > 1e-300 else 1e-300)
            h *= d * (c if abs(c) > 1e-300 else 1e-300)
            aa = -(p + m) * (p + q + m) * x / ((p + m2) * (p + m2 + 1))
            d = 1 + aa * d
            c = 1 + aa / c
            d = 1 / (d if abs(d) > 1e-300 else 1e-300)
            de = d * c
            h *= de
            if abs(de - 1) < 3e-12:
                break
        return h

    def betai(p: float, q: float, x: float) -> float:
        if x <= 0:
            return 0.0
        if x >= 1:
            return 1.0
        bt = math.exp(lgamma(p + q) - lgamma(p) - lgamma(q)
                      + p * math.log(x) + q * math.log(1 - x))
        if x < (p + 1) / (p + q + 2):
            return bt * betacf(x, p, q) / p
        return 1 - bt * betacf(1 - x, q, p) / q

    def t_cdf(x: float, v: float) -> float:
        pr = 0.5 * betai(v / 2, 0.5, v / (v + x * x))
        return 1 - pr if x > 0 else pr

    lo_b, hi_b = 0.0, 100.0
    for _ in range(200):
        mid = (lo_b + hi_b) / 2
        if t_cdf(mid, df) < 0.975:
            lo_b = mid
        else:
            hi_b = mid
    tcrit = (lo_b + hi_b) / 2
    diff = ma - mb
    # "in percent of the pooled mean" (protocol 9): the mean over all values of
    # both groups. At equal group sizes this equals (ma+mb)/2; at unequal sizes
    # the two readings differ, and this is the one we declare.
    ref = (sum(a) + sum(b)) / (na + nb)
    return (100 * diff / ref,
            100 * (diff - tcrit * se) / ref,
            100 * (diff + tcrit * se) / ref)


# ---------------------------------------------------------------- Nachlauf

BOOT_N = 10_000
BOOT_SEED = 20260905          # Protocol 8.3; decision history in section 14
NL_TYPES = ["SEQUENTIAL", "UUIDV1", "UUIDV7", "ULID", "ULID_MONOTONIC", "UUIDV4"]
NL_ORDERED = NL_TYPES[:5]


def nl_load(stem: str) -> list[dict]:
    """Per-run records of one arm, from the run log.

    The `_raw.csv` export rounds to two decimals; `<stem>.csv.runs.jsonl` holds
    the full precision the orchestrator recorded. The rounding is not always
    harmless: in the cached arm it flips one pair of I/O-per-operation values
    and changes that rank test. The log is the primary record, so we read it.
    """
    path = DATA / f"{stem}.csv.runs.jsonl"
    if not path.exists():
        fail(f"missing run log {path}")
        return []
    import json
    return [json.loads(l) for l in path.read_text().splitlines() if l.strip()]


def nl_runs(recs: list[dict], met: str) -> dict[str, list[float]]:
    """Per-run values of one metric, ordered by repetition."""
    out: dict[str, list[tuple[int, float]]] = {}
    for r in recs:
        out.setdefault(r["key_type"].upper(), []).append(
            (r["run"], float(r["metrics"][met])))
    return {t: [v for _, v in sorted(vs)] for t, vs in out.items()}


def nl_io_per_op(recs: list[dict]) -> dict[str, list[float]]:
    """I/O per operation, formed per run (consolidated protocol 7)."""
    rio, thr = nl_runs(recs, "read_iops"), nl_runs(recs, "read_throughput")
    return {t: [a / b if b else float("nan")
                for a, b in zip(rio[t], thr[t])] for t in rio}


def boot_ratio_ci(a: list[float], b: list[float], level: float,
                  seed: int = BOOT_SEED) -> tuple[float, float]:
    """Percentile bootstrap CI for median(a)/median(b), 10k resamples."""
    rng = np.random.default_rng(seed)
    aa, bb = np.asarray(a, float), np.asarray(b, float)
    out = np.empty(BOOT_N)
    for i in range(BOOT_N):
        out[i] = (np.median(rng.choice(aa, len(aa), replace=True))
                  / np.median(rng.choice(bb, len(bb), replace=True)))
    lo = (1 - level) / 2 * 100
    return float(np.percentile(out, lo)), float(np.percentile(out, 100 - lo))


# ---------------------------------------------------------------- macros

MACROS: list[tuple[str, str]] = []


def macro(name: str, value) -> None:
    if any(c.isdigit() for c in name):
        fail(f"macro name contains digit (LaTeX-invalid): {name}")
    if any(n == name for n, _ in MACROS):
        fail(f"duplicate macro {name}")
    MACROS.append((name, str(value)))


def fmt(x: float, nd: int = 2) -> str:
    return f"{x:.{nd}f}"


def fmt_int(x: float) -> str:
    """Integer with thousands separator, American style (41,109). Used for
    counts that appear in running text; review Stoerl, 11.09.2026."""
    return f"{round(x):,}"


# ---------------------------------------------------------------- pipeline

def main() -> int:
    print("== gen_numbers: loading ==")
    r4 = load_raw("cassandra_50m_read_mem4g_n3_raw.csv")
    r32 = load_raw("cassandra_50m_read_mem32g_n3_raw.csv")
    ri = load_raw("cassandra_50m_insert_mem4g_n3_raw.csv")
    r1 = load_raw("cassandra_cluster_1m_raw.csv")
    if FAILURES:
        return finish()

    # structural checks: cache_hit == index_hit everywhere (known code identity)
    for name, rows in [("4g", r4), ("32g", r32), ("1m", r1)]:
        ch, ih = {}, {}
        for r in rows:
            key = (r["Scenario"], r["KeyType"])
            if r["Metric"] == "cache_hit_ratio":
                ch[key] = (r["Run1"], r["Run2"], r["Run3"])
            if r["Metric"] == "index_hit_ratio":
                ih[key] = (r["Run1"], r["Run2"], r["Run3"])
        check(all(ch[k] == ih[k] for k in ch if k in ih),
              f"{name}: cache_hit_ratio != index_hit_ratio (code identity broken?)")

    # ---------------- 4g read
    thr = runs(r4, "read_performance", "read_throughput")
    p50 = runs(r4, "read_performance", "p50_latency_us")
    p95 = runs(r4, "read_performance", "p95_latency_us")
    p99 = runs(r4, "read_performance", "p99_latency_us")
    bfp = runs(r4, "read_performance", "bloom_filter_fp")
    rio = runs(r4, "read_performance", "read_iops")
    for d, n in [(thr, "4g thr"), (p50, "4g p50"), (p95, "4g p95"),
                 (p99, "4g p99"), (bfp, "4g bloom"), (rio, "4g rio")]:
        expect_full(d, n)
    if FAILURES:
        return finish()

    v4 = thr["UUIDV4"]
    pool = [v for t in ORDERED for v in thr[t]]
    check(max(v4) < min(pool), "4g: complete separation on throughput broken")
    p_exact = exact_pooled(v4, pool, "less")
    check(abs(p_exact - 1 / 816) < 1e-9, "4g: exact pooled p != 1/816 despite separation")

    macro("ReadPExact", fmt(p_exact, 5))
    macro("ReadPExactFrac", "1/816")
    macro("ReadDeltaCliff", fmt(abs(cliffs(v4, pool)), 1))
    macro("ReadVFourMedianTput", fmt(st.median(v4)))
    macro("ReadPooledMedianTput", fmt(st.median(pool)))
    macro("ReadRatioTput", fmt(st.median(pool) / st.median(v4), 2))
    macro("ReadVFourMedianPFifty", fmt(st.median(p50["UUIDV4"]), 0))
    macro("ReadPooledMedianPFifty",
          fmt(st.median([v for t in ORDERED for v in p50[t]]), 0))
    macro("ReadRatioPFifty",
          fmt(st.median(p50["UUIDV4"]) / st.median([v for t in ORDERED for v in p50[t]]), 2))
    macro("ReadRatioPNinetyFive",
          fmt(st.median(p95["UUIDV4"]) / st.median([v for t in ORDERED for v in p95[t]]), 2))
    macro("ReadRatioPNinetyNine",
          fmt(st.median(p99["UUIDV4"]) / st.median([v for t in ORDERED for v in p99[t]]), 2))
    macro("ReadBloomVFourMin", fmt(min(bfp["UUIDV4"]), 0))
    macro("ReadBloomVFourMax", fmt(max(bfp["UUIDV4"]), 0))
    check(all(v == 0 for t in ORDERED for v in bfp[t]),
          "4g: ordered bloom_fp not all zero")
    macro("ReadOrderedMedianSpreadPct",
          fmt(100 * (max(st.median(thr[t]) for t in ORDERED)
                     / min(st.median(thr[t]) for t in ORDERED) - 1), 0))

    h, pk = kw_perm([thr[t] for t in TYPES], 1)
    macro("ReadKwH", fmt(h, 2))
    macro("ReadKwPerm", fmt(pk, 3))
    h99, pk99 = kw_perm([p99[t] for t in TYPES], 2)
    macro("ReadKwPermPNinetyNine", fmt(pk99, 3))
    hio, pkio = kw_perm([rio[t] for t in TYPES], 3)
    macro("ReadKwPermIops", fmt(pkio, 3))

    # ---------------- insert
    ithr = runs(ri, "insert_performance", "throughput")
    ip50 = runs(ri, "insert_performance", "p50_latency_us")
    its = runs(ri, "insert_performance", "table_size_mb")
    for d, n in [(ithr, "ins thr"), (ip50, "ins p50"), (its, "ins tsize")]:
        expect_full(d, n)
    if FAILURES:
        return finish()

    meds = {t: st.median(ithr[t]) for t in TYPES}
    macro("InsMedianMin", fmt(min(meds.values()), 0))
    macro("InsMedianMax", fmt(max(meds.values()), 0))
    macro("InsSpreadPct", fmt(100 * (max(meds.values()) / min(meds.values()) - 1), 1))
    d_pct, lo_pct, hi_pct = welch_ci95(ithr["UUIDV4"], [v for t in ORDERED for v in ithr[t]])
    macro("InsDiffPct", fmt(d_pct, 2))
    macro("InsCiLoPct", fmt(lo_pct, 2))
    macro("InsCiHiPct", fmt(hi_pct, 2))
    macro("InsExcludePct", fmt(max(abs(lo_pct), abs(hi_pct)), 1))
    d_pct, lo_pct, hi_pct = welch_ci95(ip50["UUIDV4"], [v for t in ORDERED for v in ip50[t]])
    macro("InsPFiftyExcludePct", fmt(max(abs(lo_pct), abs(hi_pct)), 1))
    ts_v4 = its["UUIDV4"]
    ts_pool = [v for t in ORDERED for v in its[t]]
    check(min(ts_v4) > max(ts_pool), "insert: table_size separation broken")
    macro("InsTsizeDiffPct", fmt(100 * (st.mean(ts_v4) / st.mean(ts_pool) - 1), 1))
    macro("InsTsizePTwoSided", fmt(exact_pooled(ts_v4, ts_pool, "two"), 5))
    macro("InsTsizeVFourVsSeqPct",
          fmt(100 * (st.median(its["UUIDV4"]) / st.median(its["SEQUENTIAL"]) - 1), 1))
    macro("InsTsizeVSevenVsSeqPct",
          fmt(100 * (st.median(its["UUIDV7"]) / st.median(its["SEQUENTIAL"]) - 1), 1))
    macro("InsTsizeVOneVsSeqPct",
          fmt(100 * (st.median(its["UUIDV1"]) / st.median(its["SEQUENTIAL"]) - 1), 1))

    # ---------------- 32g control
    thr32 = runs(r32, "read_performance", "read_throughput")
    rio32 = runs(r32, "read_performance", "read_iops")
    expect_full(thr32, "32g thr")
    expect_full(rio32, "32g rio")
    if FAILURES:
        return finish()
    check(all(v == 0 for t in TYPES for v in rio32[t]), "32g: read_iops not all zero")
    macro("CtrlIopsZeroRuns", "18")
    d_pct, lo_pct, hi_pct = welch_ci95(thr32["UUIDV4"], [v for t in ORDERED for v in thr32[t]])
    macro("CtrlDiffPct", fmt(d_pct, 1))
    macro("CtrlCiLoPct", fmt(lo_pct, 1))
    macro("CtrlCiHiPct", fmt(hi_pct, 1))
    meds32 = {t: st.median(thr32[t]) for t in TYPES}
    macro("CtrlMedianMin", fmt(min(meds32.values()), 1))
    macro("CtrlMedianMax", fmt(max(meds32.values()), 1))

    # ---------------- 1M cluster
    scen_metric = {"insert_performance": "throughput",
                   "read_performance": "read_throughput",
                   "update_performance": "update_throughput",
                   "mixed_insert_heavy": "overall_throughput",
                   "mixed_read_update": "overall_throughput"}
    scen_macro = {"insert_performance": "Ins", "read_performance": "Read",
                  "update_performance": "Upd", "mixed_insert_heavy": "Mih",
                  "mixed_read_update": "Mru"}
    for i, (scen, met) in enumerate(scen_metric.items()):
        d = runs(r1, scen, met)
        expect_full(d, f"1m {scen}")
        if FAILURES:
            return finish()
        m = {t: st.median(d[t]) for t in TYPES}
        spread = 100 * (max(m.values()) / min(m.values()) - 1)
        _, pkw = kw_perm([d[t] for t in TYPES], 10 + i)
        tag = scen_macro[scen]
        macro(f"OneM{tag}SpreadPct", fmt(spread, 1))
        macro(f"OneM{tag}KwPerm", fmt(pkw, 3))

    zero = nonzero = 0
    mx = 0.0
    for scen in ["read_performance", "update_performance",
                 "mixed_insert_heavy", "mixed_read_update"]:
        d = runs(r1, scen, "read_iops")
        expect_full(d, f"1m rio {scen}")
        for t in TYPES:
            for v in d[t]:
                if v == 0:
                    zero += 1
                else:
                    nonzero += 1
                    mx = max(mx, v)
    check(zero + nonzero == 72, f"1m: expected 72 non-insert runs, got {zero + nonzero}")
    check(mx <= 0.2, f"1m: read_iops max {mx} exceeds cached-regime expectation")
    macro("OneMIopsRuns", str(zero + nonzero))
    macro("OneMIopsZero", str(zero))
    macro("OneMIopsMax", fmt(mx, 2))

    sst = runs(r1, "insert_performance", "sstable_count")
    expect_full(sst, "1m sstable")
    check(all(v == 3.0 for t in TYPES for v in sst[t]), "1m: sstable_count != 3 somewhere")
    macro("OneMSstablePerNode", "1")

    ts1 = runs(r1, "insert_performance", "table_size_mb")
    expect_full(ts1, "1m tsize")
    macro("OneMTsizeVFourVsSeqPct",
          fmt(100 * (st.median(ts1["UUIDV4"]) / st.median(ts1["SEQUENTIAL"]) - 1), 1))
    macro("OneMTsizeVSevenVsSeqPct",
          fmt(100 * (st.median(ts1["UUIDV7"]) / st.median(ts1["SEQUENTIAL"]) - 1), 1))
    macro("OneMTsizeVOneVsSeqPct",
          fmt(100 * (st.median(ts1["UUIDV1"]) / st.median(ts1["SEQUENTIAL"]) - 1), 1))

    # combinatorial constants (sample size table)
    macro("PairMinTwoSidedNThree", fmt(2 / math.comb(6, 3), 3))
    macro("PairMinTwoSidedNFive", fmt(2 / math.comb(10, 5), 4))
    macro("PairMinOneSidedNFive", fmt(1 / math.comb(10, 5), 5))

    nachlauf()
    return finish()


# ---------------------------------------------------------------- Nachlauf

# (stem, macro tag, expected runs, one-sided?) per protocol section 3 and 8.2
NL_ARMS = [
    ("nachlauf_a1_read_50m_4g_n5", "NlAOne", 5, True),
    ("nachlauf_a2_read_50m_32g_n5", "NlATwo", 5, False),
    ("nachlauf_a3_read_50m_4g_single_n5", "NlAThree", 5, True),
]

# metric -> (macro word, direction in which UUIDv4 is worse)
NL_METRICS = [
    ("read_throughput", "Tput", "less"),
    ("p50_latency_us", "PFifty", "greater"),
    ("p95_latency_us", "PNinetyFive", "greater"),
    ("p99_latency_us", "PNinetyNine", "greater"),
]


def nl_test(v4: list[float], ref: list[float], worse: str,
            one_sided: bool) -> tuple[float, float]:
    """Exact rank-sum p and Cliff's delta, direction per protocol 8.2."""
    alt = worse if one_sided else "two"
    return exact_pooled(v4, ref, alt), cliffs(v4, ref)


def nl_arm(rows: list[dict], tag: str, one_sided: bool) -> None:
    """Unpaired primary and supplementary analyses (protocol 8.1-8.6)."""
    series = {w: (nl_runs(rows, m), d) for m, w, d in NL_METRICS}
    series["IoOp"] = (nl_io_per_op(rows), "greater")

    for word, (data, worse) in series.items():
        v4, v7 = data["UUIDV4"], data["UUIDV7"]
        pool = [v for t in NL_ORDERED for v in data[t]]
        p, d = nl_test(v4, v7, worse, one_sided)
        if word == "Tput":
            # Label assignments; complete separation attains 1/count one-sided.
            macro(f"{tag}TputAssignments",
                  str(math.comb(len(v4) + len(v7), len(v4))))
        macro(f"{tag}{word}P", fmt(p, 5))
        macro(f"{tag}{word}Delta", fmt(d, 2))
        macro(f"{tag}{word}MedVFour", fmt(st.median(v4), 2))
        macro(f"{tag}{word}MedVSeven", fmt(st.median(v7), 2))
        # A ratio needs a non-zero reference. In a fully cached arm read_iops
        # is zero for every run, so IO/op is zero everywhere and the ratio is
        # 0/0. That zero is the finding (protocol 7, floor of the abort rule),
        # so the endpoint is reported as degenerate rather than as a number.
        # A ratio needs a non-zero reference, and a bootstrap ratio needs a
        # reference in which no resample can be all zeros. In the cached arm
        # some runs have zero read I/O, so I/O per operation is zero there;
        # that zero is the finding (protocol 7, floor of the abort rule), not
        # a defect. Report what is defined and say so where nothing is.
        if st.median(v7) == 0:
            macro(f"{tag}{word}Ratio", "undefined")
            print(f"   NOTE {tag}{word}: reference median is 0, "
                  f"ratio undefined")
        else:
            macro(f"{tag}{word}Ratio", fmt(st.median(v4) / st.median(v7), 3))
            # The natural sentence is "v7 reaches N times the throughput of
            # v4", so the reciprocal needs a macro too; otherwise someone
            # types it by hand.
            macro(f"{tag}{word}RatioInv", fmt(st.median(v7) / st.median(v4), 2))
        if min(v7) == 0:
            for lab in ("Ninety", "NinetyFive"):
                macro(f"{tag}{word}Ci{lab}Lo", "undefined")
                macro(f"{tag}{word}Ci{lab}Hi", "undefined")
            print(f"   NOTE {tag}{word}: reference group contains a zero, "
                  f"bootstrap ratio CI undefined "
                  f"({sum(1 for v in v7 if v == 0)} of {len(v7)} runs)")
        else:
            for level, lab in ((0.90, "Ninety"), (0.95, "NinetyFive")):
                lo, hi = boot_ratio_ci(v4, v7, level)
                macro(f"{tag}{word}Ci{lab}Lo", fmt(lo, 3))
                macro(f"{tag}{word}Ci{lab}Hi", fmt(hi, 3))
        # Supplementary model-dependent class contrast (protocol 8.6).
        # A3 has no pooled rank comparison in the implemented analysis.
        if tag == "NlAThree":
            continue
        pp, pd = nl_test(v4, pool, worse, one_sided)
        macro(f"{tag}{word}PooledP", fmt(pp, 5))
        macro(f"{tag}{word}PooledDelta", fmt(pd, 2))
        macro(f"{tag}{word}PooledRatio",
              "undefined" if st.median(pool) == 0
              else fmt(st.median(v4) / st.median(pool), 3))

    thr = nl_runs(rows, "read_throughput")
    h, pk = kw_perm([thr[t] for t in NL_TYPES], 20)
    macro(f"{tag}KwH", fmt(h, 2))
    # Small simulated p-values are displayed as a bound (protocol 8.4).
    # Numerical reproduction alone does not validate simulation precision.
    macro(f"{tag}KwPerm", "<0.001" if pk < 0.001 else fmt(pk, 3))

    # IO/op exclusion rule, consolidated protocol 7: per arm, on the median
    rio, wio = nl_runs(rows, "read_iops"), nl_runs(rows, "write_iops")
    all_r = [v for t in NL_TYPES for v in rio[t]]
    all_w = [v for t in NL_TYPES for v in wio[t]]
    # A run with read_iops = 0 and positive write_iops exceeds any threshold;
    # it is the most extreme case the rule is meant to catch, so it enters the
    # median as an exceedance rather than being dropped.
    ratios = [float("inf") if r == 0 else w / r for w, r in zip(all_w, all_r)]
    med_r = st.median(all_r)
    med_ratio = st.median(ratios)
    n_inf = sum(1 for x in ratios if x == float("inf"))
    macro(f"{tag}MedReadIops", fmt(med_r, 2))
    macro(f"{tag}WriteReadPct",
          "infinite" if med_ratio == float("inf") else fmt(100 * med_ratio, 2))
    macro(f"{tag}ZeroReadIopsRuns", str(n_inf))
    fires = med_r > 1 and med_ratio > 0.05
    macro(f"{tag}IoOpRule", "dropped" if fires else "kept")
    sst = nl_runs(rows, "sstable_count")
    macro(f"{tag}MedSstables",
          fmt(st.median([v for t in NL_TYPES for v in sst[t]]), 0))
    nl_bloom(rows, tag)


def nl_bloom(rows: list[dict], tag: str) -> None:
    """Descriptive Bloom false-positive counts (protocol 7: no test).

    The counter is a clamped net delta over the run window, so a zero means
    "no net increase", not "no probe". Reported per arm as run counts and
    extremes; the June pattern (positives only for UUIDv4) is checked
    against the campaign here rather than assumed (ledger row 15).
    """
    bfp = nl_runs(rows, "bloom_filter_fp")
    v4 = bfp["UUIDV4"]
    ordered = [v for t in NL_ORDERED for v in bfp[t]]
    macro(f"{tag}BloomRunsPerType", str(len(v4)))
    macro(f"{tag}BloomOrderedRuns", str(len(ordered)))
    macro(f"{tag}BloomVFourPositiveRuns", str(sum(1 for v in v4 if v > 0)))
    macro(f"{tag}BloomOrderedPositiveRuns",
          str(sum(1 for v in ordered if v > 0)))
    macro(f"{tag}BloomVFourMin", fmt(min(v4), 0))
    macro(f"{tag}BloomVFourMax", fmt(max(v4), 0))
    macro(f"{tag}BloomVFourMed", fmt(st.median(v4), 0))
    macro(f"{tag}BloomOrderedMax", fmt(max(ordered), 0))


def nachlauf() -> None:
    """Campaign analyses as documented in the consolidated protocol."""
    print("\n== nachlauf: registered analysis of the four arms ==")
    for stem, tag, n, one_sided in NL_ARMS:
        recs = nl_load(stem)
        if not recs:
            return
        counts = {len(v) for v in nl_runs(recs, "read_throughput").values()}
        check(counts == {n}, f"{stem}: run counts {counts}, expected {n}")
        check(set(nl_runs(recs, "read_throughput")) == set(NL_TYPES),
              f"{stem}: key types incomplete")
        if FAILURES:
            return
        nl_arm(recs, tag, one_sided)
        print(f"   {tag}: {n} runs per type, "
              f"{'one-sided' if one_sided else 'two-sided'}")

    # A2 Welch 95% intervals (protocol 9); retain historical margin/Verdict
    # calculations internally, without reporting equivalence judgments.
    # Percentages use the observed mean over all values of both groups.
    # I/O macros remain internal when the endpoint is excluded; no I/O inference.
    r2 = nl_load("nachlauf_a2_read_50m_32g_n5")
    series = {w: nl_runs(r2, m) for m, w, _ in NL_METRICS}
    series["IoOp"] = nl_io_per_op(r2)
    for word, data in series.items():
        v4 = data["UUIDV4"]
        for ref, rtag in ((data["UUIDV7"], "VSeven"),
                          ([v for t in NL_ORDERED for v in data[t]], "Pooled")):
            d, lo, hi = welch_ci95(v4, ref)
            half = (hi - lo) / 2
            macro(f"NlATwoEquiv{word}{rtag}DiffPct", fmt(d, 2))
            macro(f"NlATwoEquiv{word}{rtag}CiLoPct", fmt(lo, 2))
            macro(f"NlATwoEquiv{word}{rtag}CiHiPct", fmt(hi, 2))
            macro(f"NlATwoEquiv{word}{rtag}Verdict",
                  "no practically relevant difference"
                  if abs(d) + half < 5.0 else "undecided")
    print("   NlATwo: bounding rule of section 9 on all endpoints, "
          "both contrasts")

    # A4 bridge, descriptive only (protocol 12): no p value
    for stem, tag in (("nachlauf_a4_bridge_head_50m_4g_n3", "NlAFourHead"),
                      ("nachlauf_a1_read_50m_4g_n5", "NlAFourNeutral")):
        thr = nl_runs(nl_load(stem), "read_throughput")
        pool = [v for t in NL_ORDERED for v in thr[t]]
        macro(f"{tag}Ratio", fmt(st.median(pool) / st.median(thr["UUIDV4"]), 2))
        lo, hi = boot_ratio_ci(pool, thr["UUIDV4"], 0.95)
        macro(f"{tag}CiLo", fmt(lo, 2))
        macro(f"{tag}CiHi", fmt(hi, 2))
    print("   NlAFour: bridge ratios per sampler, descriptive, no p value")
    nl_bloom(nl_load("nachlauf_a4_bridge_head_50m_4g_n3"), "NlAFour")

    nachlauf_insert()

    # A2 descriptive: medians of the four schemes that are neither of the
    # two CQL-uuid-typed ones. The pair contrast and the class contrast
    # disagree in this arm, and the reader needs to see where the four
    # others sit to judge why. Medians only, no test (protocol 8.6).
    thr2 = nl_runs(r2, "read_throughput")
    others = [st.median(thr2[t]) for t in NL_TYPES if t not in ("UUIDV4", "UUIDV7")]
    macro("NlATwoTputMedOtherMin", fmt(min(others), 2))
    macro("NlATwoTputMedOtherMax", fmt(max(others), 2))
    check(max(st.median(thr2["UUIDV4"]), st.median(thr2["UUIDV7"])) < min(others),
          "A2: the two uuid-typed schemes are not below the other four by median")


def nachlauf_insert() -> None:
    """A5, insert 50M at 4 GB, n=5: Welch intervals (protocol 9).
    Descriptive medians, spread and size tiers accompany supplementary values.
    Historical margin/Equiv calculations remain internal, not equivalence claims.
    A5 was performed; the optional-file branch below is retained for compatibility.
    """
    stem = "nachlauf_a5_insert_50m_4g_n5"
    if not (DATA / f"{stem}.csv.runs.jsonl").exists():
        print(f"   NOTE A5: {stem} not present, insert arm skipped")
        return
    recs = nl_load(stem)
    thr = nl_runs(recs, "throughput")
    p50 = nl_runs(recs, "p50_latency_us")
    tsz = nl_runs(recs, "table_size_mb")
    check(set(thr) == set(NL_TYPES), "A5: key types incomplete")
    check({len(v) for v in thr.values()} == {5}, "A5: expected 5 runs per type")
    if FAILURES:
        return
    tag = "NlAFive"
    meds = {t: st.median(thr[t]) for t in NL_TYPES}
    macro(f"{tag}TputMedMin", fmt_int(min(meds.values())))
    macro(f"{tag}TputMedMax", fmt_int(max(meds.values())))
    macro(f"{tag}TputSpreadPct",
          fmt(100 * (max(meds.values()) / min(meds.values()) - 1), 1))
    macro(f"{tag}TputMedVFour", fmt_int(meds["UUIDV4"]))
    macro(f"{tag}TputMedVSeven", fmt_int(meds["UUIDV7"]))
    macro(f"{tag}TputRankVFour",
          str(1 + sum(1 for t in NL_TYPES if meds[t] > meds["UUIDV4"])))
    # A5 Welch 95% intervals (protocol 9), both contrasts, throughput and p50.
    # Historical margin/Verdict calculations remain internal, not reported.
    for word, data in (("Tput", thr), ("PFifty", p50)):
        v4 = data["UUIDV4"]
        for ref, rtag in ((data["UUIDV7"], "VSeven"),
                          ([v for t in NL_ORDERED for v in data[t]], "Pooled")):
            d, lo, hi = welch_ci95(v4, ref)
            half = (hi - lo) / 2
            macro(f"{tag}Equiv{word}{rtag}DiffPct", fmt(d, 2))
            macro(f"{tag}Equiv{word}{rtag}CiLoPct", fmt(lo, 2))
            macro(f"{tag}Equiv{word}{rtag}CiHiPct", fmt(hi, 2))
            macro(f"{tag}Equiv{word}{rtag}ExcludePct",
                  fmt(max(abs(lo), abs(hi)), 1))
            macro(f"{tag}Equiv{word}{rtag}Verdict",
                  "no practically relevant difference"
                  if abs(d) + half < 5.0 else "undecided")
    # Table size: tiers against sequential, and v4 against the pooled class
    ts_v4 = tsz["UUIDV4"]
    ts_pool = [v for t in NL_ORDERED for v in tsz[t]]
    macro(f"{tag}TsizeSeparated",
          "complete" if min(ts_v4) > max(ts_pool) else "overlapping")
    macro(f"{tag}TsizeDiffPct", fmt(100 * (st.mean(ts_v4) / st.mean(ts_pool) - 1), 1))
    macro(f"{tag}TsizePTwoSided", fmt(exact_pooled(ts_v4, ts_pool, "two"), 5))
    for t, lab in (("UUIDV4", "VFour"), ("UUIDV7", "VSeven"), ("UUIDV1", "VOne"),
                   ("ULID", "Ulid"), ("ULID_MONOTONIC", "UlidMono")):
        macro(f"{tag}Tsize{lab}VsSeqPct",
              fmt(100 * (st.median(tsz[t]) / st.median(tsz["SEQUENTIAL"]) - 1), 1))
    # Row losses in the load: reported per protocol 11.5 (type-dependent
    # loss is a finding), as share of the table and as UUIDv4's rank.
    lost = nl_runs(recs, "failed")
    all_lost = [v for t in NL_TYPES for v in lost[t]]
    macro(f"{tag}LostRowsMax", fmt_int(max(all_lost)))
    macro(f"{tag}LostRowsMaxPct", fmt(100 * max(all_lost) / 50_000_000, 4))
    med_lost = {t: st.median(lost[t]) for t in NL_TYPES}
    macro(f"{tag}LostRankVFour",
          str(1 + sum(1 for t in NL_TYPES if med_lost[t] > med_lost["UUIDV4"])))
    print("   NlAFive: bounding rule of section 9 on throughput and p50, "
          "table-size tiers and row losses descriptive")


def finish() -> int:
    if FAILURES:
        print(f"\n== gen_numbers: {len(FAILURES)} check(s) FAILED, "
              f"no output written ==")
        return 1
    OUT.parent.mkdir(exist_ok=True)
    with open(OUT, "w") as fh:
        fh.write("% AUTO-GENERATED by scripts/gen_numbers.py - do not edit.\n")
        fh.write(f"% permutation seed={PERM_SEED}, draws={PERM_N}\n")
        fh.write(f"% bootstrap seed={BOOT_SEED}, resamples={BOOT_N} "
                 f"(Amendment 4, point 2)\n")
        for name, val in MACROS:
            fh.write(f"\\newcommand{{\\Num{name}}}{{{val}}}\n")
    print(f"\n== gen_numbers: OK, {len(MACROS)} macros -> {OUT.relative_to(ROOT)} ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
