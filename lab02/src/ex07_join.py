"""Lab 2, Exercise 7: join curated_q1 to the zone lookup on DOLocationID,
the strategy Spark chooses on its own ("auto") versus the other strategy ("forced").

Query: q1.join(zones, q1.DOLocationID == zones.LocationID, "left")
         .groupBy("Borough").agg(F.count("*").alias("trips"))   (collected)
"auto": defaults (AQE on, default thresholds), no hint. The join operator is read from
the final executed plan after one run. "forced": if auto is BroadcastHashJoin, sort merge
is forced by setting spark.sql.autoBroadcastJoinThreshold=-1 AND
spark.sql.adaptive.autoBroadcastJoinThreshold=-1; if auto is a shuffle join, broadcast is
forced with F.broadcast(zones).
Protocol: one warm-up each, then three scored rounds auto,forced | forced,auto | auto,forced.
Refuses to run unless runs/ex07_prediction.txt exists.
Run: spark-submit ~/bda/lab02/src/ex07_join.py
"""
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1, REF_ZONES

HERE = Path(__file__).resolve().parent.parent
PRED = HERE / "runs" / "ex07_prediction.txt"
if not PRED.exists() or not PRED.read_text().strip():
    sys.exit("STOP: runs/ex07_prediction.txt is missing or empty; the prediction file must exist first.")

spark = (SparkSession.builder.appName("lab02-ex07-join").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

q1 = spark.read.parquet(CURATED_Q1)
zones = (spark.read.option("header", True).csv(REF_ZONES)
         .withColumn("LocationID", F.col("LocationID").cast("int")))

THR, ATHR = "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.autoBroadcastJoinThreshold"
JOIN_OPS = r"(BroadcastHashJoin|SortMergeJoin|ShuffledHashJoin|BroadcastNestedLoopJoin|CartesianProduct)"

print()
print(f"  prediction file: {PRED.name} ({PRED.stat().st_size} bytes), present before measuring")
print(f"  settings: master local[4]")
print(f"    {THR} = {spark.conf.get(THR)}")
try:
    athr_value = spark.conf.get(ATHR)
except Exception:                      # optional byte-size setting with no default value
    athr_value = "<not set> (falls back to spark.sql.autoBroadcastJoinThreshold)"
print(f"    {ATHR} = {athr_value}")
print(f"    spark.sql.adaptive.enabled = {spark.conf.get('spark.sql.adaptive.enabled')}")
print(f"    spark.sql.shuffle.partitions = {spark.conf.get('spark.sql.shuffle.partitions')}")
print()


def final_plan(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    return plan


def final_part(plan):
    """The part of an AQE plan above '== Initial Plan ==' (the final plan)."""
    return plan.split("== Initial Plan ==")[0]


def join_op(plan):
    m = re.search(JOIN_OPS, final_part(plan))
    return m.group(1) if m else "none found"


def build(mode, forced_how):
    if mode == "auto" or forced_how == "broadcast":
        spark.conf.unset(THR)
        spark.conf.unset(ATHR)
    else:                                         # forced sort merge
        spark.conf.set(THR, "-1")
        spark.conf.set(ATHR, "-1")
    z = F.broadcast(zones) if (mode == "forced" and forced_how == "broadcast") else zones
    return (q1.join(z, q1.DOLocationID == z.LocationID, "left")
              .groupBy("Borough").agg(F.count("*").alias("trips")))


def run(mode, forced_how=None):
    df = build(mode, forced_how)
    t0 = time.perf_counter()
    rows = df.collect()
    dt = time.perf_counter() - t0
    return dt, sorted(((r["Borough"] or "", r["trips"]) for r in rows)), df


# ---- 1. auto: one run, read the join operator from the final plan -------------
_, _, auto_df = run("auto")
auto_plan = final_plan(auto_df)
auto_op = join_op(auto_plan)
print(f"  auto run done; final plan contains isFinalPlan=true: {'yes' if 'isFinalPlan=true' in auto_plan else 'no'}")
print(f"  join operator chosen automatically (final plan): {auto_op}")
if auto_op == "BroadcastHashJoin":
    forced_how = "sortmerge"
    print(f"  forced: sort merge join, because auto is BroadcastHashJoin; set {THR}=-1 and {ATHR}=-1")
else:
    forced_how = "broadcast"
    print(f"  forced: broadcast with F.broadcast(zones), because auto is {auto_op} (a shuffle join)")
print()

# ---- 2. timing -----------------------------------------------------------------
MODES = ["auto", "forced"]
warm = {m: run(m, forced_how)[0] for m in MODES}
times = {m: [] for m in MODES}
results = {m: None for m in MODES}
last_df = {}
orders = [["auto", "forced"], ["forced", "auto"], ["auto", "forced"]]
for r, order in enumerate(orders):
    for m in order:
        dt, rows, df = run(m, forced_how)
        times[m].append(dt)
        if results[m] is None:
            results[m] = rows
        elif results[m] != rows:
            print(f"  WARNING: {m} returned different rows in round {r + 1}")
        last_df[m] = df
spark.conf.unset(THR)
spark.conf.unset(ATHR)

plans = {m: final_plan(last_df[m]) for m in MODES}
ops = {m: join_op(plans[m]) for m in MODES}
label = {m: f"{m} ({ops[m]})" for m in MODES}

print(f"  round order: {' | '.join(','.join(o) for o in orders)}")
print()
print(f"  {'strategy':<30}{'warm-up s':>10}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}")
best = {}
for m in MODES:
    t = times[m]
    best[m] = min(t)
    print(f"  {label[m]:<30}{warm[m]:>10.3f}{t[0]:>10.3f}{t[1]:>10.3f}{t[2]:>10.3f}{best[m]:>10.3f}")
slow, fast = sorted(MODES, key=lambda m: best[m], reverse=True)
print()
print(f"  slower: {label[slow]}, faster: {label[fast]}")
print(f"  best(slower) / best(faster) = {best[slow] / best[fast]:.2f}")
print()
print(f"  results identical between auto and forced: {'PASS' if results['auto'] == results['forced'] else 'FAIL'}")
print(f"  trips per Borough (dropoff zone):")
for b, n in results["auto"]:
    print(f"    {b or 'null (no matching zone)':<28}{n:>12,}")
total = sum(n for _, n in results["auto"])
print(f"    {'total':<28}{total:>12,}")
print()

# ---- 3. plans ------------------------------------------------------------------
stamp = datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S UTC %Y")
with (HERE / "runs" / "ex07_plans.log").open("w") as f:
    f.write(f"{stamp}\n")
    f.write("Exercise 7: final executed plans (executedPlan().toString(), taken after .collect() "
            "on the DataFrame of the last scored run)\n\n")
    for m in MODES:
        f.write(f"==== {label[m]} ====\n{plans[m]}\n\n")
print("  plans written to runs/ex07_plans.log")
for m in MODES:
    print(f"  ---- {label[m]}: final plan, isFinalPlan=true: {'yes' if 'isFinalPlan=true' in plans[m] else 'no'} ----")
    for line in final_part(plans[m]).splitlines():
        if re.search(JOIN_OPS, line) or "Exchange" in line:
            print(f"    {line.strip()}")
print()

out = HERE / "out" / "parts" / "ex07.csv"
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["exercise", "measurement", "setting", "warmup_s", "run1_s", "run2_s", "run3_s", "best_s", "source_log"])
    for m in MODES:
        t = times[m]
        w.writerow(["ex07", "join zones on DOLocationID, trips per Borough", label[m],
                    f"{warm[m]:.3f}", f"{t[0]:.3f}", f"{t[1]:.3f}", f"{t[2]:.3f}", f"{best[m]:.3f}", "runs/ex07_join.log"])
print("  timings written to out/parts/ex07.csv")
print()

spark.stop()
