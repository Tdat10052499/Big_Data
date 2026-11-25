"""Lab 2, Exercise 9: the uneven task. groupBy(PULocationID).count() with AQE off and
8 shuffle partitions, then keep the Spark UI open so src/ex09_capture.py can read the
per-task numbers and take screenshots (same open-UI pattern as src/ex04_job.py).

From the collected rows: number of groups, the largest group, the median group size,
largest/median, the largest group as a share of curated_q1. For each of the 8 shuffle
partitions: which PULocationIDs land in it, computed with pmod(hash(PULocationID), 8)
(the Murmur3 hash, seed 42, that Spark's hashpartitioning uses).
The measured job runs in job group "ex09"; helper jobs run in group "ex09-aux".
Writes runs/ex09_ui.ready (UI address) and waits for runs/ex09_ui.stop (max 20 min).
Run: spark-submit ~/bda/lab02/src/ex09_skew.py
"""
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

RUNS = Path(__file__).resolve().parent.parent / "runs"
READY, STOP = RUNS / "ex09_ui.ready", RUNS / "ex09_ui.stop"
for f in (READY, STOP):
    f.unlink(missing_ok=True)

spark = (SparkSession.builder.appName("lab02-ex09-skew").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "8")

q1 = spark.read.parquet(CURATED_Q1)

print()
print(f"  settings: master {sc.master}, spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  application id {sc.applicationId}, UI {sc.uiWebUrl}")
print()

agg = q1.groupBy("PULocationID").count()
sc.setJobGroup("ex09", "ex09: groupBy(PULocationID).count() collected")
rows = agg.collect()

sc.setJobGroup("ex09-aux", "ex09 helper jobs (not measured)")
total = q1.count()

counts = sorted(r["count"] for r in rows)
n = len(counts)
top = max(rows, key=lambda r: r["count"])
if n % 2 == 0:
    m1, m2 = counts[n // 2 - 1], counts[n // 2]
    median = (m1 + m2) / 2
    med_text = (f"{median:,} (even number of groups: mean of the two middle values "
                f"{m1:,} and {m2:,}, positions {n // 2} and {n // 2 + 1} of {n} sorted ascending)")
else:
    median = counts[n // 2]
    med_text = f"{median:,} (odd number of groups: the middle value, position {n // 2 + 1} of {n})"

print(f"  number of groups (distinct PULocationID): {n:,}")
print(f"  largest group: PULocationID {top['PULocationID']} with {top['count']:,} trips")
print(f"  median group size: {med_text}")
print(f"  largest / median = {top['count'] / median:,.1f}")
print(f"  largest group = {100 * top['count'] / total:.1f}% of the {total:,} curated_q1 rows")
print()

# ---- which keys land in which of the 8 shuffle partitions ----------------------
keyed = (spark.createDataFrame(rows, schema=agg.schema)       # same column type as in q1, so the same hash
              .withColumn("part", F.expr("pmod(hash(PULocationID), 8)")))
print(f"  PULocationID column type: {dict(agg.dtypes)['PULocationID']}; partition = pmod(hash(PULocationID), 8)")
parts = {r["part"]: r for r in keyed.groupBy("part").agg(
    F.count("*").alias("keys"), F.sum("count").alias("trips"),
    F.max(F.when(F.col("PULocationID") == top["PULocationID"], 1).otherwise(0)).alias("has_top")).collect()}
print(f"  {'partition':>9}{'keys':>7}{'trips':>13}  contains largest group (PULocationID {top['PULocationID']})")
sum_trips = 0
for p in range(8):
    r = parts.get(p)
    keys, trips, has = (r["keys"], r["trips"], r["has_top"] == 1) if r else (0, 0, False)
    sum_trips += trips
    print(f"  {p:>9}{keys:>7}{trips:>13,}  {'yes' if has else 'no'}")
print(f"  {'sum':>9}{sum(parts[p]['keys'] for p in parts):>7}{sum_trips:>13,}")
print(f"  sum of trips over the 8 partitions equals the curated_q1 row count ({total:,}): "
      f"{'PASS' if sum_trips == total else 'FAIL'}")
print()

READY.write_text(sc.uiWebUrl + "\n")
print(f"  UI is up; waiting for {STOP.name} (max 20 min)", flush=True)
deadline = time.time() + 20 * 60
while not STOP.exists() and time.time() < deadline:
    time.sleep(1)
STOP.unlink(missing_ok=True)
READY.unlink(missing_ok=True)
spark.stop()
