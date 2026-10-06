#!/usr/bin/env python3
"""Validity gate for the Nachlauf campaign, protocol sections 3, 4, 6 and 11.

Runs BEFORE any evaluation (11.4: completeness and validity over all runs of an
arm first, then analysis). It answers three questions and nothing else:

  1. Did we run the documented design? (arm parameters asserted
     against sections 3 and 4, not merely printed)
  2. Is the measurement path the same across arms? (binary checksums)
  3. Which runs are valid? (consolidated protocol 11.1-11.3)

It deliberately reports no throughput, latency or I/O magnitudes; those belong
to the evaluation step. Run durations are not printed either, because at
concurrency 1 they are a proxy for throughput (decision history: protocol 14).

Rules applied, documented in consolidated protocol 11:
  - (failed + not_found) / attempted >= 0.001                 -> run invalid
  - insert_failed / num-records >= 0.001                     -> run invalid
  - (num-ops - attempted) / num-ops >= 0.001                  -> run invalid
  - t_fetch_s > 0 in the neutral-sampler arms (violates 5.3)  -> run invalid
  - io_valid == 0  -> NOT invalid; drops only the IO/op endpoint (11.1)
  - arm invalid if more than two runs invalid, or if not every key type reaches
    the run count planned for that arm (11.3, as corrected)
  - arm-level explanation criterion (11.2): the summed
    target deficit must sit within three Poisson standard deviations of the
    deficit expected from the write-side losses

Usage: python3 scripts/check_validity.py
"""

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"

TYPES = {"sequential", "uuidv1", "uuidv7", "ulid", "ulid_monotonic", "uuidv4"}

# Declared in protocol section 3. stem -> (label, n, memory, heap, newgen,
# nodes, replication factor, head sampling)
ARMS = [
    ("nachlauf_a1_read_50m_4g_n5", "A1  read 50M 4g",
     5, "4g", "2G", "512M", 3, 3, False),
    ("nachlauf_a2_read_50m_32g_n5", "A2  read 50M 32g",
     5, "32g", "8G", "2G", 3, 3, False),
    ("nachlauf_a3_read_50m_4g_single_n5", "A3  read 50M 4g single",
     5, "4g", "2G", "512M", 1, 1, False),
    ("nachlauf_a4_bridge_head_50m_4g_n3", "A4  bridge, head sampler",
     3, "4g", "2G", "512M", 3, 3, True),
]

# A5 (insert 50M, 4g, n=5) was performed; preserve optional-file compatibility.
# It has no read phase: target-deficit and fetch rules of 11.1 do not apply,
# but the row-loss rule does.
INSERT_ARM = ("nachlauf_a5_insert_50m_4g_n5", "A5  insert 50M 4g",
              5, "4g", "2G", "512M", 3, 3, False)
if (DATA / f"{INSERT_ARM[0]}.csv.runs.jsonl").exists():
    ARMS.append(INSERT_ARM)
INSERT_STEMS = {INSERT_ARM[0]}

# Declared in protocol section 4, identical for every arm.
COMMON = {"num-records": "50000000", "num-ops": "100000",
          "num-buckets": "50000", "batch-size": "100",
          "consistency": "local_one", "cassandra-cpus": "8"}

MARGIN = 0.001   # Consolidated protocol 11.1; decision history in section 14
SIGMA = 3.0      # Consolidated protocol 11.2

problems: list[str] = []
notes: list[str] = []


def fail(msg: str) -> None:
    problems.append(msg)


def norm(v):
    """The manifest stores numbers and booleans as strings; compare by value."""
    if isinstance(v, str):
        low = v.strip().lower()
        if low in ("true", "false"):
            return low == "true"
        try:
            return int(low)
        except ValueError:
            return v
    return v


def load(stem: str):
    meta = json.loads((DATA / f"{stem}.csv.meta.json").read_text())
    runs = [json.loads(l) for l in
            (DATA / f"{stem}.csv.runs.jsonl").read_text().splitlines() if l.strip()]
    return meta, runs


def main() -> int:
    print("== 1. Messpfad: dieselben Binaries ueber alle Arme? ==")
    sigs, images = set(), set()
    for stem, label, *_ in ARMS:
        meta, _ = load(stem)
        sigs.add((meta["orchestrator_md5"], meta["workload_md5"]))
        images.add(meta["effective_cluster"]["image"])
        print(f"  {label:<26} commit {meta['commit'][:10]}  "
              f"orch {meta['orchestrator_md5'][:8]}  "
              f"workload {meta['workload_md5'][:8]}  dirty={meta['working_tree_dirty']}")
    if len(sigs) == 1:
        print("  -> beide Binaries ueber alle Arme bit-gleich")
    else:
        fail(f"Binaries unterscheiden sich zwischen den Armen ({len(sigs)} Staende)")
    if len(images) == 1:
        print(f"  -> Cassandra-Image per Digest gepinnt und identisch")
    else:
        fail(f"verschiedene Cassandra-Images: {images}")
    notes.append("Der Manifestwert `commit` ist `git rev-parse HEAD` zur "
                 "LAUFZEIT, nicht zur Buildzeit. Die Bindung Binary->Quelle "
                 "ist seit 06.09. durch bit-identischen Nachbau belegt "
                 "(data/PROVENANCE.md); `go version -m` zeigt hier nichts, "
                 "weil beide Binaries ueber eine Datei gebaut werden.")

    print("\n== 2. Armparameter gegen Protokoll 3 und 4 ==")
    for stem, label, n, mem, heap, newgen, nodes, rf, head in ARMS:
        meta, runs = load(stem)
        f, e = meta["flags"], meta["effective_cluster"]
        checks = [
            (f["cassandra-memory"], mem, "memory"),
            (f["cassandra-heap"], heap, "heap"),
            (f["cassandra-newgen"], newgen, "newgen"),
            (e["node_count"], nodes, "node_count"),
            (e["replication_factor"], rf, "replication_factor"),
            (e["head_sampling"], head, "head_sampling"),
            (int(f["num-runs"]), n, "num-runs"),
            (meta.get("prep_insert_connections"), 8, "prep insert connections"),
        ] + [(f.get(k), v, k) for k, v in COMMON.items()
             if not (stem in INSERT_STEMS and k == "num-ops")]
        if stem not in INSERT_STEMS:
            checks.append((meta.get("measured_connections_read_update"), 1,
                           "measured connections"))
        else:
            # Insert scenario: eight writers are the measured concurrency
            # (runbook A5), and the June run used the same setting.
            checks.append((f.get("connections"), "8", "connections"))
        bad = [f"{name}: {got!r} statt {want!r}" for got, want, name in checks
               if norm(got) != norm(want)]
        print(f"  {label:<26} {'ok' if not bad else 'ABWEICHUNG'}")
        for b in bad:
            fail(f"{label}: {b}")
        if len(runs) != len(meta.get("runs", runs)):
            fail(f"{label}: Manifest und Lauf-Log verschieden lang")

    print("\n== 3. Gueltigkeitsgate nach 11.1 (Amendments 1 und 2) ==")
    for stem, label, n, *_rest in ARMS:
        head = _rest[-1]
        meta, runs = load(stem)
        num_ops = int(meta["flags"].get("num-ops", 0))  # absent in the insert arm
        num_rec = int(meta["flags"]["num-records"])
        invalid, io_dropped, per_type = [], [], {}
        obs = exp = chi = 0.0
        deficit_without_loss = 0
        if stem in INSERT_STEMS:
            for r in runs:
                m, tag = r["metrics"], f"{r['key_type']}/run{r['run']}"
                per_type.setdefault(r["key_type"], []).append(r["run"])
                # The insert scenario reports lost rows as `failed`; the read
                # arms carry the same count as `insert_failed`.
                lost = m.get("insert_failed", m.get("failed", 0))
                if lost / num_rec >= MARGIN:
                    invalid.append((tag, "Zeilenverlustrate"))
                if m.get("io_valid", 1) == 0:
                    io_dropped.append(tag)
            print(f"\n  {label}: {len(runs)} Laeufe, {len(per_type)} Typen, "
                  f"{len(invalid)} ungueltig (Insert-Arm: nur Zeilenverlustregel)")
            for tag, why in invalid:
                print(f"      {tag}: {why}")
            if len(invalid) > 2:
                fail(f"{label}: mehr als zwei ungueltige Laeufe (11.3)")
            if set(per_type) != TYPES:
                fail(f"{label}: Typmenge {sorted(per_type)} != die sechs Schemata")
            bad_n = {t: len(v) for t, v in per_type.items() if len(v) != n}
            if bad_n:
                fail(f"{label}: Typen ohne geplante Laufzahl {n}: {bad_n}")
            continue
        for r in runs:
            m, tag = r["metrics"], f"{r['key_type']}/run{r['run']}"
            per_type.setdefault(r["key_type"], []).append(r["run"])
            reasons = []
            att = m["attempted"]
            if m["succeeded"] + m["failed"] + m["not_found"] != att:
                reasons.append("succeeded+failed+not_found != attempted")
            if att > num_ops:
                reasons.append(f"attempted={att} > num-ops={num_ops}")
            if att and (m["failed"] + m["not_found"]) / att >= MARGIN:
                reasons.append("Operationsfehlerrate")
            if m["insert_failed"] / num_rec >= MARGIN:
                reasons.append("Zeilenverlustrate")
            if (num_ops - att) / num_ops >= MARGIN:
                reasons.append("Zielfehlbetrag")
            if not head and m["t_fetch_s"] > 0:
                reasons.append(f"t_fetch_s={m['t_fetch_s']} (verletzt 5.3)")
            if head and m["t_fetch_s"] <= 0:
                reasons.append("t_fetch_s=0, aber Head-Sampler deklariert")
            if reasons:
                invalid.append((tag, "; ".join(reasons)))
            if m["io_valid"] == 0:
                io_dropped.append(tag)
            d, lam = num_ops - att, m["insert_failed"] * num_ops / num_rec
            obs += d
            exp += lam
            chi += (d - lam) ** 2 / lam if lam > 0 else 0.0
            if d > 0 and m["insert_failed"] == 0:
                deficit_without_loss += 1

        z = (obs - exp) / exp ** 0.5 if exp > 0 else 0.0
        print(f"\n  {label}: {len(runs)} Laeufe, {len(per_type)} Typen, "
              f"{len(invalid)} ungueltig")
        for tag, why in invalid:
            print(f"      {tag}: {why}")
        print(f"    Zielfehlbetrag {int(obs)} beobachtet, {exp:.1f} erwartet, "
              f"z = {z:+.2f}, Dispersion chi2 = {chi:.1f} bei {len(runs)} df")
        print(f"    Fehlbetraege ohne Schreibverlust: {deficit_without_loss}")
        print(f"    io_valid=0 (nur IO/op faellt): {len(io_dropped) or 'keine'}")
        if deficit_without_loss:
            fail(f"{label}: {deficit_without_loss} Lauf/Laeufe mit Fehlbetrag "
                 f"ohne Schreibverlust")
        if head:
            print("    (Head-Sampler zieht nicht ueber Einfuegepositionen, "
                  "z ist hier ohne Bedeutung)")
        elif abs(z) > SIGMA:
            fail(f"{label}: Zielfehlbetrag nicht durch Schreibverlust erklaert "
                 f"(z = {z:+.2f})")
        if len(invalid) > 2:
            fail(f"{label}: mehr als zwei ungueltige Laeufe (11.3)")
        if set(per_type) != TYPES:
            fail(f"{label}: Typmenge {sorted(per_type)} != die sechs Schemata")
        bad_n = {t: len(v) for t, v in per_type.items() if len(v) != n}
        if bad_n:
            fail(f"{label}: Typen ohne geplante Laufzahl {n}: {bad_n}")

    print("\n== 4. Randomisierung und Seeds (Protokoll 6) ==")
    for stem, label, n, *_ in ARMS:
        meta, runs = load(stem)
        samples = {r["sample_seed"] for r in runs}
        by_rep = {}
        for r in runs:
            by_rep.setdefault(r["run"], []).append((r["order_index"], r["key_type"]))
        perm_ok = all(sorted(i for i, _ in v) == list(range(6)) for v in by_rep.values())
        orders = {r["run"]: r["order_seed"] for r in runs}
        print(f"  {label:<26} {len(samples)}/{len(runs)} verschiedene sample_seeds, "
              f"Permutation je Wiederholung {'ok' if perm_ok else 'FEHLT'}, "
              f"{len(set(orders.values()))} order_seeds fuer {len(orders)} Wiederholungen")
        if len(samples) != len(runs):
            fail(f"{label}: sample_seed nicht eindeutig je Lauf")
        if not perm_ok:
            fail(f"{label}: order_index ist keine Permutation der sechs Typen")

    print("\n" + "=" * 62)
    for nte in notes:
        print(f"HINWEIS: {nte}")
    if problems:
        print(f"\n== check_validity: {len(problems)} Problem(e) ==")
        for p in problems:
            print(f"  FAIL: {p}")
        return 1
    print("\n== check_validity: alle Arme bestehen das Gate ==")
    return 0


if __name__ == "__main__":
    sys.exit(main())
