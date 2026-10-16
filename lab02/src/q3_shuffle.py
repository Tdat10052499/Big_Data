"""Lab 2, Sections 6 and 7: what the shuffle costs, and how many partitions to use.
Run:  spark-submit ~/bda/lab02/src/q3_shuffle.py
"""
import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab02-shuffle").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")


def best_of_three(fn):
    """Warm up once, then take the fastest of three. The fastest run is the one
    least disturbed by other work on the machine."""
    fn()
    return min(timed(fn) for _ in range(3))


def timed(fn):
    t0 = time.time(); fn(); return time.time() - t0


def wide():
    # Zone by zone by hour. Many groups, so the shuffle has real work to do.
    return (q1.groupBy("PULocationID", "DOLocationID", "pickup_hour")
              .agg(F.count("*").alias("trips"))
              .agg(F.count("*")).collect())


print()
groups = q1.groupBy("PULocationID", "DOLocationID", "pickup_hour").count().count()
print(f"  groups produced by the wide aggregation: {groups:,}")
print()

# ---- with AQE switched off, so the setting is the setting -----------------
spark.conf.set("spark.sql.adaptive.enabled", "false")
print("  Adaptive Query Execution OFF")
print(f"  {'shuffle partitions':>20}   {'best of 3':>9}")
for p in [1, 4, 8, 50, 200, 800]:
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    print(f"  {p:>20}   {best_of_three(wide):>7.2f} s")

# ---- with AQE switched back on -------------------------------------------
spark.conf.set("spark.sql.adaptive.enabled", "true")
print()
print("  Adaptive Query Execution ON")
for p in [1, 200, 800]:
    spark.conf.set("spark.sql.shuffle.partitions", str(p))
    print(f"  {p:>20}   {best_of_three(wide):>7.2f} s")

# ---- what AQE actually did ------------------------------------------------
spark.conf.set("spark.sql.shuffle.partitions", "800")
q = q1.groupBy("PULocationID", "DOLocationID", "pickup_hour").agg(F.count("*").alias("n"))
q.collect()
print()
print("  ---- the plan AQE ended up running ----")
for line in q._jdf.queryExecution().executedPlan().toString().splitlines()[:6]:
    if line.strip():
        print("  " + line[:96])
print()

spark.stop()
