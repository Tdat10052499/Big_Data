"""Lab 3, Exercise 9 supplement summary; applies runs/ex09b_rule.txt mechanically.
Part A: out/latency_parts/ex09b_k*.csv, runs/ex09b_k*_producer*.log, runs/ex09b_k*_load.log
Part B: out/latency_parts/ex09b_backlog_r*.csv
'batches 2-8' = positions 2..8 of the first 8 non-empty batches of a run, in batch order.
Run:  ~/bda/venv/bin/python src/ex09b_summary.py [A|B]   (default: both)
"""
import csv, os, re, statistics, sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
NPROC = os.cpu_count()
which = sys.argv[1] if len(sys.argv) > 1 else "AB"


def eight(f):
    return sorted(csv.DictReader(open(f)), key=lambda r: int(r["batch"]))[:8]


A, B = [], []
if "A" in which:
    T = "taxi-live"
    for f in BASE.glob("out/latency_parts/ex09b_k*.csv"):
        k = int(re.search(r"ex09b_k(\d+)\.csv", f.name).group(1))
        rows = eight(f); b = rows[1:8]
        Ps = []
        for p in sorted(BASE.glob(f"runs/ex09b_k{k}_producer*.log")):
            m = re.search(r"achieved rate\s+([\d.]+) events per second", p.read_text())
            Ps.append(float(m.group(1)) if m else float("nan"))
        R = 5000 * k; P = sum(Ps)
        I = statistics.mean(float(r["input_rows_per_s"]) for r in b)
        O = statistics.mean(float(r["processed_rows_per_s"]) for r in b)
        Tm = statistics.mean(int(r["trigger_ms"]) for r in b)
        lag = [float(r["avg_lag_s_precise"]) for r in b]
        ups = sum(1 for x, y in zip(lag, lag[1:]) if y > x)
        load = [float(x) for x in re.findall(r"load average: ([\d.]+),", (BASE / f"runs/ex09b_k{k}_load.log").read_text())]
        A.append(dict(k=k, R=R, P=P, np=len(Ps), I=I, O=O, T=Tm, l2=lag[0], l8=lag[-1], ups=ups,
                      a=P < 0.95 * R, b=(ups >= 5 and lag[-1] - lag[0] > 3) or Tm > 3000,
                      load=max(load) if load else float("nan"), n=len(rows)))
    A.sort(key=lambda d: d["k"])
    print(f"  Part A (live), topic {T}, trigger 3 s, k producers at 5,000 events/s each; rule: runs/ex09b_rule.txt")
    print("  batches 2-8 = positions 2..8 of the first 8 non-empty batches of each run")
    print(f"  {'k':>3} {'R':>6} {'P':>9} {'P/R':>6} {'I':>9} {'O':>9} {'trig ms':>8} {'lag b2':>7} {'lag b8':>7} {'ups/6':>5} {'(a)':>4} {'(b)':>4} {'max load1':>9}")
    for d in A:
        print(f"  {d['k']:>3} {d['R']:>6} {d['P']:>9.1f} {d['P']/d['R']:>6.3f} {d['I']:>9.1f} {d['O']:>9.1f} {d['T']:>8.1f} "
              f"{d['l2']:>7.3f} {d['l8']:>7.3f} {d['ups']:>5} {('yes' if d['a'] else 'no'):>4} {('yes' if d['b'] else 'no'):>4} "
              f"{d['load']:>5.2f}/{NPROC}   topic {T}" + ("" if d["np"] == d["k"] else f"  ({d['np']} producer logs)")
              + ("" if d["n"] == 8 else f"  (only {d['n']} non-empty batches)"))
    print("  (a) = (a') of the supplement rule: P < 0.95 R;  max load1 = highest 1-minute load average in runs/ex09b_k<k>_load.log / nproc")
    print()
if "B" in which:
    T = "taxi-live-backlog"
    for f in BASE.glob("out/latency_parts/ex09b_backlog_r*.csv"):
        R = int(re.search(r"ex09b_backlog_r(\d+)\.csv", f.name).group(1))
        rows = eight(f); b = rows[1:8]
        tms = [int(r["trigger_ms"]) for r in b]
        B.append(dict(R=R, mo=3 * R, rows=statistics.mean(int(r["rows"]) for r in b), T=statistics.mean(tms), Tmax=max(tms),
                      O=statistics.mean(float(r["processed_rows_per_s"]) for r in b), c=statistics.mean(tms) > 3000, n=len(rows)))
    B.sort(key=lambda d: d["R"])
    print(f"  Part B (backlog), topic {T}, trigger 3 s, startingOffsets earliest, maxOffsetsPerTrigger = 3 x R")
    print(f"  {'R':>7} {'maxOffsets':>10} {'rows/batch':>10} {'trig ms':>8} {'max trig':>8} {'O':>9} {'(c)':>4}")
    for d in B:
        print(f"  {d['R']:>7} {d['mo']:>10} {d['rows']:>10.1f} {d['T']:>8.1f} {d['Tmax']:>8} {d['O']:>9.1f} {('yes' if d['c'] else 'no'):>4}   topic {T}"
              + ("" if d["n"] == 8 else f"  (only {d['n']} non-empty batches)"))
    c = [d for d in B if d["c"]]
    if c:
        print(f"  query's own limit (lowest R with (c)), topic {T}: R = {c[0]['R']}")
    elif B:
        print(f"  query's own limit, topic {T}: not reached up to R = {B[-1]['R']}")
    print()
if which == "AB" and A:
    fb = next((d for d in A if d["b"]), None)
    fa = next((d for d in A if d["a"]), None)
    if fb and (not fa or fb["k"] < fa["k"]):
        res = f"yes: (b) first holds at k = {fb['k']} (R = {fb['R']}); (a') " + (f"first holds at k = {fa['k']}" if fa else "never holds")
    elif fa:
        res = f"no: (a') first holds at k = {fa['k']} (R = {fa['R']}); (b) " + (f"first holds at k = {fb['k']}" if fb else f"not reached up to k = {A[-1]['k']}")
    else:
        res = f"neither (a') nor (b) holds up to k = {A[-1]['k']}"
    print(f"  Part A, topic taxi-live: (b) reached before (a')? {res}")
