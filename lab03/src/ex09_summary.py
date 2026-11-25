"""Lab 3, Exercise 9 summary: applies runs/ex09_rule.txt mechanically to
out/latency_parts/ex09_r*.csv and runs/ex09_r*_producer.log.
'batches 2-8' = positions 2..8 of the first 8 non-empty batches of a run, in batch order.
Run:  ~/bda/venv/bin/python src/ex09_summary.py
"""
import csv, re, statistics
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
TOPIC = "taxi-live"
runs = []
for f in BASE.glob("out/latency_parts/ex09_r*.csv"):
    R = int(re.search(r"ex09_r(\d+)\.csv", f.name).group(1))
    rows = sorted(csv.DictReader(open(f)), key=lambda r: int(r["batch"]))[:8]
    plog = (BASE / "runs" / f"ex09_r{R}_producer.log").read_text()
    m = re.search(r"achieved rate\s+([\d.]+) events per second", plog)
    P = float(m.group(1)) if m else float("nan")
    b = rows[1:8]
    I = statistics.mean(float(r["input_rows_per_s"]) for r in b)
    O = statistics.mean(float(r["processed_rows_per_s"]) for r in b)
    T = statistics.mean(int(r["trigger_ms"]) for r in b)
    lag = [float(r["avg_lag_s_precise"]) for r in b]
    ups = sum(1 for x, y in zip(lag, lag[1:]) if y > x)
    a = P < 0.95 * R
    bb = (ups >= 5 and lag[-1] - lag[0] > 3) or T > 3000
    runs.append(dict(R=R, P=P, I=I, O=O, T=T, l2=lag[0], l8=lag[-1], ups=ups, a=a, b=bb, n=len(rows)))
runs.sort(key=lambda d: d["R"])

print(f"  Exercise 9, topic {TOPIC}, trigger 3 s; rule: runs/ex09_rule.txt")
print("  batches 2-8 = positions 2..8 of the first 8 non-empty batches of each run")
print(f"  {'R':>6} {'P':>8} {'P/R':>6} {'I':>8} {'O':>9} {'trig ms':>8} {'lag b2':>7} {'lag b8':>7} {'ups/6':>5} {'(a)':>4} {'(b)':>4}")
for d in runs:
    print(f"  {d['R']:>6} {d['P']:>8.1f} {d['P']/d['R']:>6.3f} {d['I']:>8.1f} {d['O']:>9.1f} {d['T']:>8.1f} "
          f"{d['l2']:>7.3f} {d['l8']:>7.3f} {d['ups']:>5} {('yes' if d['a'] else 'no'):>4} {('yes' if d['b'] else 'no'):>4}"
          f"   topic {TOPIC}" + ("" if d["n"] == 8 else f"  (only {d['n']} non-empty batches)"))
fb = [d for d in runs if d["b"]]
if fb:
    f0 = fb[0]
    lim = [d for d in runs if d["a"] and d["R"] < f0["R"]]
    if lim:
        print(f"  producer/machine limit first: (a) holds at R = {', '.join(str(d['R']) for d in lim)}, below the falling-behind rate")
    print(f"  falling-behind rate (lowest R where (b) holds), topic {TOPIC}: R = {f0['R']}")
    print(f"  processing rate Spark reported at R = {f0['R']} (mean processedRowsPerSecond, batches 2-8): {f0['O']:.1f} rows/s")
else:
    lim = [d for d in runs if d["a"]]
    if lim:
        print(f"  producer/machine limit: (a) holds at R = {', '.join(str(d['R']) for d in lim)}")
    print(f"  falling-behind rate, topic {TOPIC}: not reached up to R = {runs[-1]['R'] if runs else 'none'}")
