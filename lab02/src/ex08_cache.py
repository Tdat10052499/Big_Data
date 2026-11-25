"""Lab 2, Exercise 8: from how many passes over a filtered DataFrame does caching pay?

f = q1.filter(fare_amount > 10). Four different passes, each ending in .collect():
  P1 count (agg(count(*)))     P2 avg fare_amount by PULocationID
  P3 sum tip_amount by payment_type   P4 max trip_distance by pickup_hour
"k passes" = P1..Pk.
Uncached(k): P1..Pk on f.
Cached(k): f.cache(), then P1..Pk (the first pass fills the cache, so the fill time is
included); f.unpersist(blocking=True) after the clock stops. Before every cached run the
cache is checked to be empty (spark.catalog.isCached on a temp view of f).
For each k: one warm-up of each mode, then three rounds U,C | C,U | U,C.
Refuses to run unless runs/ex08_prediction.txt exists.
Run: spark-submit ~/bda/lab02/src/ex08_cache.py
"""
import csv
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

HERE = Path(__file__).resolve().parent.parent
PRED = HERE / "runs" / "ex08_prediction.txt"
if not PRED.exists() or not PRED.read_text().strip():
    sys.exit("STOP: runs/ex08_prediction.txt is missing or empty; the prediction file must exist first.")

spark = (SparkSession.builder.appName("lab02-ex08-cache").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

q1 = spark.read.parquet(CURATED_Q1)
f = q1.filter(F.col("fare_amount") > 10)
f.createOrReplaceTempView("ex08_f")      # lets spark.catalog.isCached see whether f's plan is cached

PASSES = [
    ("P1", "count(*)", lambda d: d.agg(F.count("*").alias("n")).collect()),
    ("P2", "avg fare_amount by PULocationID", lambda d: d.groupBy("PULocationID").agg(F.avg("fare_amount")).collect()),
    ("P3", "sum tip_amount by payment_type", lambda d: d.groupBy("payment_type").agg(F.sum("tip_amount")).collect()),
    ("P4", "max trip_distance by pickup_hour", lambda d: d.groupBy("pickup_hour").agg(F.max("trip_distance")).collect()),
]
seen = {p[0]: set() for p in PASSES}      # P1: the count; P2-P4: number of result rows


def do_passes(k):
    for name, _, fn in PASSES[:k]:
        rows = fn(f)
        seen[name].add(rows[0]["n"] if name == "P1" else len(rows))


def uncached(k):
    t0 = time.perf_counter()
    do_passes(k)
    return time.perf_counter() - t0


def cached(k):
    if spark.catalog.isCached("ex08_f"):
        sys.exit("STOP: the cache was not empty before a cached run")
    t0 = time.perf_counter()
    f.cache()
    do_passes(k)
    dt = time.perf_counter() - t0
    f.unpersist(blocking=True)
    return dt


total = q1.count()
nf = f.count()
print()
print(f"  prediction file: {PRED.name} ({PRED.stat().st_size} bytes), present before measuring")
print(f"  settings: master local[4], spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  f = curated_q1.filter(fare_amount > 10): {nf:,} rows = {100 * nf / total:.1f}% of the {total:,} curated_q1 rows")
print(f"  cache empty at start (spark.catalog.isCached('ex08_f')): {not spark.catalog.isCached('ex08_f')}")
for name, desc, _ in PASSES:
    print(f"    {name}: {desc}")
print()

MODES = {"uncached": uncached, "cached": cached}
orders = [["uncached", "cached"], ["cached", "uncached"], ["uncached", "cached"]]
res = {}
for k in range(1, 5):
    warm = {m: MODES[m](k) for m in MODES}
    times = {m: [] for m in MODES}
    for order in orders:
        for m in order:
            times[m].append(MODES[m](k))
    for m in MODES:
        t = times[m]
        res[(k, m)] = (warm[m], t, min(t), max(t) - min(t))
    print(f"  k={k} done", flush=True)
print()

print(f"  round order for every k: {' | '.join(','.join(o) for o in orders)}")
print(f"  {'k':>2}  {'mode':<9}{'warm-up s':>10}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}{'spread s':>10}")
for k in range(1, 5):
    for m in MODES:
        w, t, b, s = res[(k, m)]
        print(f"  {k:>2}  {m:<9}{w:>10.3f}{t[0]:>10.3f}{t[1]:>10.3f}{t[2]:>10.3f}{b:>10.3f}{s:>10.3f}")
print()
print(f"  {'k':>2}  {'best(uncached)/best(cached)':>28}  {'|difference| s':>15}  {'larger spread s':>16}  difference > larger spread")
cross = None
for k in range(1, 5):
    bu, su = res[(k, "uncached")][2], res[(k, "uncached")][3]
    bc, sc_ = res[(k, "cached")][2], res[(k, "cached")][3]
    if cross is None and bc < bu:
        cross = k
    d, sp = abs(bu - bc), max(su, sc_)
    print(f"  {k:>2}  {bu / bc:>28.2f}  {d:>15.3f}  {sp:>16.3f}  {'yes' if d > sp else 'no'}")
print()
print("  crossing point (smallest k where best(cached) < best(uncached)): "
      + (f"k = {cross}" if cross else "none for k = 1..4"))
print()
print(f"  pass results in every run: P1 count {', '.join(f'{x:,}' for x in sorted(seen['P1']))}; "
      f"P2 rows {sorted(seen['P2'])}; P3 rows {sorted(seen['P3'])}; P4 rows {sorted(seen['P4'])}")
print(f"  same result in every run: {'PASS' if all(len(v) == 1 for v in seen.values()) else 'FAIL'}"
      f"  (P1 count equals f.count(): {'PASS' if seen['P1'] == {nf} else 'FAIL'})")
print(f"  cache empty at end: {not spark.catalog.isCached('ex08_f')}")
print()

out = HERE / "out" / "parts" / "ex08.csv"
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["exercise", "measurement", "setting", "warmup_s", "run1_s", "run2_s", "run3_s", "best_s", "source_log"])
    for k in range(1, 5):
        for m in MODES:
            wu, t, b, _ = res[(k, m)]
            w.writerow(["ex08", f"{k} passes (P1..P{k}) on fare_amount>10", m,
                        f"{wu:.3f}", f"{t[0]:.3f}", f"{t[1]:.3f}", f"{t[2]:.3f}", f"{b:.3f}", "runs/ex08_cache.log"])
print("  timings written to out/parts/ex08.csv")
print()

spark.stop()
