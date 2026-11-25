"""Lab 2, Exercise 10: apply the replication rule (runs/ex10_replication_rule.txt) mechanically.

Timings are read from out/parts/ex10.csv, out/parts/ex10_rep2.csv, out/parts/ex10_rep3.csv
(written by src/ex10_faster.py; the values are the 3-decimal numbers printed in its logs).
"Plan differs from B" is read from the three logs runs/ex10_faster{,_rep2,_rep3}.log.
Nothing is typed by hand.
Run: ~/bda/venv/bin/python src/ex10_replicate.py
"""
import csv
import re
import statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
REPS = [("1", "out/parts/ex10.csv", "runs/ex10_faster.log"),
        ("2", "out/parts/ex10_rep2.csv", "runs/ex10_faster_rep2.log"),
        ("3", "out/parts/ex10_rep3.csv", "runs/ex10_faster_rep3.log")]
NAMES = ["B", "V1", "V2", "V3"]

data = {}       # (rep, variant) -> (runs [3 floats], best float, text of runs as in the csv)
differs = {}    # (rep, variant) -> "yes"/"no"
for rep, part, log in REPS:
    with (HERE / part).open(newline="") as f:
        for row in csv.DictReader(f):
            v = row["setting"].split(":")[0]
            runs_txt = [row["run1_s"], row["run2_s"], row["run3_s"]]
            data[(rep, v)] = ([float(x) for x in runs_txt], float(row["best_s"]), runs_txt, row["best_s"])
    for m in re.finditer(r"final plan of (V\d): .*differs from B's plan \(ids stripped\): (yes|no)",
                         (HERE / log).read_text()):
        differs[(rep, m.group(1))] = m.group(2)

print()
print("  sources: " + "; ".join(f"replication {r}: {p} + {l}" for r, p, l in REPS))
print()
print(f"  {'variant':<8}{'rep':>4}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}  plan differs from B")
for v in NAMES:
    for rep, _, _ in REPS:
        _, _, rt, bt = data[(rep, v)]
        print(f"  {v:<8}{rep:>4}{rt[0]:>10}{rt[1]:>10}{rt[2]:>10}{bt:>10}  {'-' if v == 'B' else differs[(rep, v)]}")
print()

pooled = {v: [x for rep, _, _ in REPS for x in data[(rep, v)][0]] for v in NAMES}
med = {v: statistics.median(pooled[v]) for v in NAMES}
print("  median of 9 runs = the 5th of the 9 pooled scored runs sorted ascending")
print(f"  B: 9 runs {sorted(pooled['B'])}, median {med['B']:.3f} s, min-max {min(pooled['B']):.3f}-{max(pooled['B']):.3f} s")
print()

verdict, timing_ok = {}, {}
for v in NAMES[1:]:
    n_best = sum(1 for rep, _, _ in REPS if data[(rep, v)][1] < data[(rep, "B")][1])
    pairs = [(a, b) for a in pooled[v] for b in pooled["B"]]
    faster = sum(1 for a, b in pairs if a < b)
    ties = sum(1 for a, b in pairs if a == b)
    share = faster / len(pairs)
    plan_all = all(differs[(rep, v)] == "yes" for rep, _, _ in REPS)
    c1, c2, c3, c4 = n_best == 3, med[v] < med["B"], share >= 0.75, plan_all
    timing_ok[v] = c1 and c2 and c3
    verdict[v] = c1 and c2 and c3 and c4
    print(f"  ---- {v} ----")
    print(f"    9 runs {sorted(pooled[v])}, min-max {min(pooled[v]):.3f}-{max(pooled[v]):.3f} s")
    print(f"    (1) replications with best({v}) < best(B): {n_best} of 3")
    print(f"    (2) median({v}) {med[v]:.3f} s, median(B) {med['B']:.3f} s, median(B)/median({v}) = {med['B'] / med[v]:.2f}")
    print(f"    (3) {v} run faster in {faster} of the {len(pairs)} ({v} run, B run) pairs = {100 * share:.1f}% of the 81 pairs"
          f" (ties: {ties})")
    print(f"    plan differs from B in replications 1/2/3: "
          f"{'/'.join(differs[(rep, v)] for rep, _, _ in REPS)}")
    print(f"    condition (1) = 3 of 3: {'yes' if c1 else 'no'}")
    print(f"    condition median({v}) < median(B): {'yes' if c2 else 'no'}")
    print(f"    condition (3) >= 75% of the 81 pairs: {'yes' if c3 else 'no'}")
    print(f"    condition plan differs from B (all three replications): {'yes' if c4 else 'no'}")
    print(f"    three timing conditions met: {'yes' if timing_ok[v] else 'no'}")
    print(f"    verdict: {v} {'counts as faster than B' if verdict[v] else 'does not count as faster than B'}")
    print()

print("  overall:")
if timing_ok["V3"]:
    print("    V3 (same plan as B, noise control) meets the three timing conditions: yes")
    print("    -> no variant is declared faster (the timing difference cannot be attributed to a plan change)")
else:
    print("    V3 (same plan as B, noise control) meets the three timing conditions: no")
    winners = [v for v in NAMES[1:] if verdict[v]]
    if winners:
        print(f"    -> faster than B over three replications: {', '.join(winners)}")
    else:
        print("    -> no variant is reliably faster than the baseline over three replications")
low = min(NAMES, key=lambda v: med[v])
print(f"  lowest median of the 9 pooled runs: {low} ({med[low]:.3f} s); medians: "
      + ", ".join(f"{v} {med[v]:.3f}" for v in NAMES))
print()
