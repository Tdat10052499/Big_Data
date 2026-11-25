"""Lab 2, Exercise 10: make one query faster, and prove it.

Baseline B (default configuration, nothing changed):
  q1.join(zones, q1.PULocationID == zones.LocationID, "left")
    .filter(trip_distance > 5).groupBy("Borough", "pickup_hour").agg(avg(fare_amount) as avg_fare)
Candidates, each changing ONE thing from B:
  V1 aggregate before the join (filter, groupBy(PULocationID, pickup_hour) sum/count,
     join zones, groupBy(Borough, pickup_hour) sum(sum)/sum(count))
  V2 spark.sql.shuffle.partitions=8 (AQE still on), query identical to B
  V3 explicit F.broadcast(zones), query identical to B
Correctness: same keys as B and avg_fare equal within relative 1e-9.
Timing: one warm-up of each, then three rounds in the orders
  B,V1,V2,V3 | V3,V2,V1,B | B,V1,V2,V3.
Winner (mechanical): the variant with the lowest best whose final plan differs from B's
(after stripping #123 expression ids and plan_id=N) and whose best is lower than
B's fastest run by more than B's spread.
Run: spark-submit ~/bda/lab02/src/ex10_faster.py
"""
import argparse
import csv
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1, REF_ZONES

HERE = Path(__file__).resolve().parent.parent
ap = argparse.ArgumentParser()
ap.add_argument("--suffix", default="", help="replication suffix for every output file, e.g. _rep2")
SUFFIX = ap.parse_args().suffix

spark = (SparkSession.builder.appName("lab02-ex10-faster").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

q1 = spark.read.parquet(CURATED_Q1)
zones = (spark.read.option("header", True).csv(REF_ZONES)
         .withColumn("LocationID", F.col("LocationID").cast("int")))

SP = "spark.sql.shuffle.partitions"
print()
print(f"  replication suffix: {SUFFIX or 'none'}")
print(f"  default configuration (baseline B):")
print(f"    spark.sql.adaptive.enabled = {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"    {SP} = {spark.conf.get(SP)}")
print(f"    spark.sql.autoBroadcastJoinThreshold = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")
print()


def q_base(z):
    return (q1.join(z, q1.PULocationID == z.LocationID, "left")
              .filter(F.col("trip_distance") > 5)
              .groupBy("Borough", "pickup_hour").agg(F.avg("fare_amount").alias("avg_fare")))


def q_v1():
    pre = (q1.filter(F.col("trip_distance") > 5)
             .groupBy("PULocationID", "pickup_hour")
             .agg(F.sum("fare_amount").alias("s"), F.count("fare_amount").alias("c")))
    return (pre.join(zones, pre.PULocationID == zones.LocationID, "left")
               .groupBy("Borough", "pickup_hour")
               .agg((F.sum("s") / F.sum("c")).alias("avg_fare")))


VARIANTS = {
    "B":  ("baseline: join, then filter, then groupBy (defaults)", lambda: q_base(zones), None),
    "V1": ("aggregate before the join", q_v1, None),
    "V2": ("spark.sql.shuffle.partitions=8 (AQE on)", lambda: q_base(zones), "8"),
    "V3": ("explicit F.broadcast(zones)", lambda: q_base(F.broadcast(zones)), None),
}
NAMES = list(VARIANTS)


def run(v):
    _, build, parts = VARIANTS[v]
    if parts:
        spark.conf.set(SP, parts)
    else:
        spark.conf.unset(SP)
    df = build()
    t0 = time.perf_counter()
    rows = df.collect()
    dt = time.perf_counter() - t0
    spark.conf.unset(SP)
    res = {(r["Borough"], r["pickup_hour"]): r["avg_fare"] for r in rows}
    return dt, res, df


for v in NAMES:
    print(f"  {v:<3} {VARIANTS[v][0]}")
print()

warm, times, results, last_df = {}, {v: [] for v in NAMES}, {}, {}
for v in NAMES:
    warm[v], results[v], _ = run(v)
orders = [NAMES, NAMES[::-1], NAMES]
for order in orders:
    for v in order:
        dt, res, df = run(v)
        times[v].append(dt)
        if res.keys() != results[v].keys():
            print(f"  WARNING: {v} returned different keys in a scored run")
        last_df[v] = df

# ---- correctness -----------------------------------------------------------------
print("  correctness against B (same keys; avg_fare equal within relative 1e-9):")
ref = results["B"]


def close(a, b):
    if a is None or b is None:
        return a is None and b is None
    return abs(a - b) <= 1e-9 * max(abs(a), abs(b), 1e-300)


for v in NAMES[1:]:
    r = results[v]
    ok = r.keys() == ref.keys() and all(close(r[k], ref[k]) for k in ref)
    print(f"    {v}: {'PASS' if ok else 'FAIL'}  ({len(r)} result rows; B has {len(ref)})")
print()

# ---- timing table -----------------------------------------------------------------
print(f"  round order: {' | '.join(','.join(o) for o in orders)}")
print(f"  {'variant':<8}{'warm-up s':>10}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}{'spread s':>10}{'best(B)/best(V)':>17}")
best = {v: min(times[v]) for v in NAMES}
spread = {v: max(times[v]) - min(times[v]) for v in NAMES}
for v in NAMES:
    t = times[v]
    print(f"  {v:<8}{warm[v]:>10.3f}{t[0]:>10.3f}{t[1]:>10.3f}{t[2]:>10.3f}{best[v]:>10.3f}{spread[v]:>10.3f}"
          f"{best['B'] / best[v]:>17.2f}")
print()

# ---- plans -------------------------------------------------------------------------
STRIP = [(re.compile(r"#\d+L?"), ""), (re.compile(r"plan_id=\d+"), "plan_id=")]


def strip(s):
    for pat, rep in STRIP:
        s = pat.sub(rep, s)
    return s


plans = {v: last_df[v]._jdf.queryExecution().executedPlan().toString() for v in NAMES}
differs = {v: strip(plans[v]) != strip(plans["B"]) for v in NAMES}
for v in NAMES:
    print(f"  final plan of {v}: isFinalPlan=true {'yes' if 'isFinalPlan=true' in plans[v] else 'no'}"
          + ("" if v == "B" else f"; differs from B's plan (ids stripped): {'yes' if differs[v] else 'no'}"))
print()

limit = best["B"] - spread["B"]
print("  rule: winner = the variant with the lowest best whose plan differs from B's and whose best is lower")
print(f"        than B's fastest run ({best['B']:.3f} s) by more than B's spread ({spread['B']:.3f} s),"
      f" i.e. best < {limit:.3f} s")
qualified = [v for v in NAMES[1:] if differs[v] and best[v] < limit]
for v in NAMES[1:]:
    print(f"    {v}: best {best[v]:.3f} s, plan differs {'yes' if differs[v] else 'no'}, "
          f"best < {limit:.3f}: {'yes' if best[v] < limit else 'no'}  -> {'qualifies' if v in qualified else 'does not qualify'}")
if qualified:
    chosen = min(qualified, key=lambda v: best[v])
    print(f"  result: winner {chosen} ({VARIANTS[chosen][0]})")
else:
    chosen = min(NAMES[1:], key=lambda v: best[v])
    print(f"  result: no variant is reliably faster than the baseline; plan files written for the fastest variant, {chosen}")
print()


def lines_of(p):
    return [l for l in p.splitlines()]


def marked(plan, other):
    other_set = {strip(l).strip() for l in other.splitlines()}
    out, n = [], 0
    for l in plan.splitlines():
        if strip(l).strip() not in other_set:
            out.append(f"\x1b[33m{l}   <== changed\x1b[0m")
            n += 1
        else:
            out.append(l)
    return "\n".join(out), n


stamp = datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S UTC %Y")
for fname, v, other, title in [(f"ex10_plan_before{SUFFIX}.log", "B", chosen, "BEFORE: baseline B"),
                               (f"ex10_plan_after{SUFFIX}.log", chosen, "B", f"AFTER: {chosen}")]:
    m, n = marked(plans[v], plans[other])
    (HERE / "runs" / fname).write_text(
        f"{stamp}\n"
        f"Exercise 10 {title} ({VARIANTS[v][0]})\n"
        f"Final executed plan (executedPlan().toString()) of the DataFrame of the last scored run.\n\n"
        f"==== Part 1: plan as returned by Spark ====\n{plans[v]}\n\n"
        f"==== Part 2: the same plan, lines not present in the {other} plan marked <== changed "
        f"(compared after stripping #ids and plan_id) ====\n{m}\n")
    print(f"  wrote runs/{fname}: plan of {v}, {n} lines marked <== changed (compared with {other})")
print()

out = HERE / "out" / "parts" / f"ex10{SUFFIX}.csv"
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["exercise", "measurement", "setting", "warmup_s", "run1_s", "run2_s", "run3_s", "best_s", "source_log"])
    for v in NAMES:
        t = times[v]
        w.writerow(["ex10", "join+filter+groupBy(Borough,pickup_hour) avg fare", f"{v}: {VARIANTS[v][0]}",
                    f"{warm[v]:.3f}", f"{t[0]:.3f}", f"{t[1]:.3f}", f"{t[2]:.3f}", f"{best[v]:.3f}", f"runs/ex10_faster{SUFFIX}.log"])
print(f"  timings written to out/parts/ex10{SUFFIX}.csv")
print()

spark.stop()
