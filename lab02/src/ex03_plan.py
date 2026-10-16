"""Lab 2, Exercise 3: read the plan of
    q1.groupBy("payment_type").agg(F.avg("tip_amount"))

Part 1 prints the formatted plan exactly as Spark returns it (taken before any action).
Part 2 prints the same plan again with the three answers marked (yellow, "<==").
The answers are read from the plan text by this script, not typed in.
Run: spark-submit ~/bda/lab02/src/ex03_plan.py
"""
import re
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

spark = (SparkSession.builder.appName("lab02-ex03-plan").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

q1 = spark.read.parquet(CURATED_Q1)
query = q1.groupBy("payment_type").agg(F.avg("tip_amount").alias("avg_tip"))

plan = spark._jvm.PythonSQLUtils.explainString(query._jdf.queryExecution(), "formatted")
lines = plan.splitlines()

print()
print(f"  settings: spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  columns in curated_q1: {len(q1.columns)}")
print()
print("  ==== Part 1: plan as printed by explain(mode='formatted') ====")
print(plan)

# ---- read the three answers from the plan text -------------------------
scan_hdr = next(i for i, l in enumerate(lines) if re.match(r"^\(\d+\) Scan parquet", l))
scan_end = next((i for i in range(scan_hdr, len(lines)) if not lines[i].strip()), len(lines))
scan_block = range(scan_hdr, scan_end)
scan_out = next(i for i in scan_block if lines[i].startswith("Output ["))
pushed = next((i for i in scan_block if lines[i].startswith("PushedFilters")), None)
n_cols = int(re.search(r"Output \[(\d+)\]", lines[scan_out]).group(1))
# Spark omits the PushedFilters line from the scan node when nothing was pushed.
pushed_list = lines[pushed].split(":", 1)[1].strip() if pushed is not None else "[]"
pushed_text = lines[pushed].strip() if pushed is not None else "no PushedFilters line in the Scan parquet node"
exchanges = [i for i, l in enumerate(lines) if re.match(r"^\(\d+\) Exchange\b", l)]

marks = {scan_out: f"[1] scan reads {n_cols} of {len(q1.columns)} columns",
         (pushed if pushed is not None else scan_hdr):
             "[2] pushed filters: " + ("none" if pushed_list == "[]" else pushed_list)}
for k, i in enumerate(exchanges, 1):
    marks[i] = f"[3] Exchange {k} of {len(exchanges)}"

print("  ==== Part 2: the same plan, answers marked by this script ====")
for i, l in enumerate(lines):
    print(f"\x1b[33m{l}   <== {marks[i]}\x1b[0m" if i in marks else l)
print()
print(f"  [1] columns read by the scan : {n_cols} of {len(q1.columns)}  ({lines[scan_out].strip()})")
print(f"  [2] filter pushed down       : {'no' if pushed_list == '[]' else 'yes'}  ({pushed_text})")
print(f"  [3] Exchange steps           : {len(exchanges)}")
print()

print("  result (action run after the plan was printed):")
for r in query.orderBy("payment_type").collect():
    avg = "null" if r["avg_tip"] is None else f"{r['avg_tip']:.4f}"
    print(f"    payment_type {r['payment_type']}   avg_tip {avg}")
print()

spark.stop()
