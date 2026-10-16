"""Lab 2, Exercise 5: shuffle partitions 2 versus 8 for the wide aggregation, AQE off.

Wide aggregation (as in q3_shuffle.py): groupBy(PULocationID, DOLocationID, pickup_hour)
.count(), then count the groups, collected to the driver.
Protocol: one warm-up run per setting (not scored), then three scored rounds. The
order inside a round alternates (2 then 8, 8 then 2, 2 then 8) so that neither
setting always runs first. The physical plan of each setting is printed, so the
Exchange line shows the partition count actually used.
Refuses to run unless runs/ex05_prediction.txt exists (the student's prediction).
Run: spark-submit ~/bda/lab02/src/ex05_partitions.py
"""
import sys
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

PRED = Path(__file__).resolve().parent.parent / "runs" / "ex05_prediction.txt"
if not PRED.exists() or not PRED.read_text().strip():
    sys.exit("STOP: runs/ex05_prediction.txt is missing or empty; the prediction must be written first.")

spark = (SparkSession.builder.appName("lab02-ex05-partitions").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.adaptive.enabled", "false")

q1 = spark.read.parquet(CURATED_Q1)
SETTINGS = [2, 8]


def query():
    return (q1.groupBy("PULocationID", "DOLocationID", "pickup_hour")
              .agg(F.count("*").alias("trips"))
              .agg(F.count("*").alias("groups")))


def run(p):
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    t0 = time.perf_counter()
    groups = query().collect()[0]["groups"]
    return time.perf_counter() - t0, groups


print()
print(f"  prediction file: {PRED.name} ({PRED.stat().st_size} bytes), present before measuring")
print(f"  settings: master local[4], spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}")
print()

for p in SETTINGS:
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    plan = query()._jdf.queryExecution().executedPlan().toString()
    print(f"  plan with spark.sql.shuffle.partitions={p} (Exchange lines):")
    for line in plan.splitlines():
        if "Exchange" in line:
            print("    " + line.strip())
print()

warm = {p: run(p)[0] for p in SETTINGS}
times = {p: [] for p in SETTINGS}
groups_seen = set()
for r in range(3):
    order = SETTINGS if r % 2 == 0 else SETTINGS[::-1]
    for p in order:
        dt, g = run(p)
        times[p].append(dt)
        groups_seen.add(g)

print(f"  groups produced by the wide aggregation: {', '.join(f'{g:,}' for g in sorted(groups_seen))}")
print(f"  round order: 2,8 | 8,2 | 2,8")
print()
print(f"  {'shuffle partitions':>18}{'warm-up s':>11}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}")
for p in SETTINGS:
    t = times[p]
    print(f"  {p:>18}{warm[p]:>11.3f}{t[0]:>10.3f}{t[1]:>10.3f}{t[2]:>10.3f}{min(t):>10.3f}")
b2, b8 = min(times[2]), min(times[8])
print()
print(f"  best(2) / best(8) = {b2 / b8:.2f}")
print()

spark.stop()
