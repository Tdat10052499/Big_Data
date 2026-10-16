"""Lab 2, Exercise 2: which line of the fragment makes Spark read data.

    a = q1.filter(F.col("fare_amount") > 10)
    b = a.select("PULocationID", "fare_amount")
    c = b.groupBy("PULocationID").count()
    d = c.orderBy(F.desc("count"))
    e = d.take(5)

Each line is timed on its own. One warm-up pass of a..e is not scored, then
three scored passes. Each line also runs under its own Spark job group, so the
log shows how many Spark jobs that line launched (a job means data was read).
Run: spark-submit ~/bda/lab02/src/ex02_lines.py
"""
import time
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

spark = (SparkSession.builder.appName("lab02-ex02-lines").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
tracker = sc.statusTracker()

q1 = spark.read.parquet(CURATED_Q1)   # schema read happens here, outside the timed lines
LINES = ["a filter", "b select", "c groupBy.count", "d orderBy", "e take(5)"]


def one_pass(tag):
    """Run lines a..e once. Returns {line: (seconds, jobs launched)} and e's result."""
    out, holder = {}, {}

    def run(name, fn):
        group = f"ex02-{tag}-{name.split()[0]}"
        sc.setJobGroup(group, name)
        t0 = time.perf_counter()
        fn()
        dt = time.perf_counter() - t0
        out[name] = (dt, len(tracker.getJobIdsForGroup(group)))

    run("a filter",        lambda: holder.__setitem__("a", q1.filter(F.col("fare_amount") > 10)))
    run("b select",        lambda: holder.__setitem__("b", holder["a"].select("PULocationID", "fare_amount")))
    run("c groupBy.count", lambda: holder.__setitem__("c", holder["b"].groupBy("PULocationID").count()))
    run("d orderBy",       lambda: holder.__setitem__("d", holder["c"].orderBy(F.desc("count"))))
    run("e take(5)",       lambda: holder.__setitem__("e", holder["d"].take(5)))
    sc.setJobGroup("ex02-idle", "idle")
    return out, holder["e"]


one_pass("warmup")
runs = [one_pass(f"run{i}") for i in (1, 2, 3)]

print()
print(f"  settings: master local[4], spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  input: curated_q1 ({q1.count():,} rows); 1 warm-up pass not scored, 3 scored passes")
print()
print(f"  {'line':<17}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}   Spark jobs per run")
for name in LINES:
    t = [r[0][name][0] for r in runs]
    j = [r[0][name][1] for r in runs]
    print(f"  {name:<17}{t[0]:>10.4f}{t[1]:>10.4f}{t[2]:>10.4f}{min(t):>10.4f}   {j[0]} / {j[1]} / {j[2]}")
print()
print("  result of e (run 3):")
for row in runs[2][1]:
    print(f"    PULocationID {row['PULocationID']:>4}   count {row['count']:,}")
print()

spark.stop()
