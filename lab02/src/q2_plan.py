"""Lab 2, Sections 4 and 5: lazy evaluation, and reading an execution plan.
Run:  spark-submit ~/bda/lab02/src/q2_plan.py
"""
import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab02-plan").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")

# ---- lazy evaluation ------------------------------------------------------
print()
print("  BUILDING the query")
t0 = time.time()
busy = (q1.filter(F.col("trip_distance") > 2)
          .select("PULocationID", "fare_amount", "trip_minutes")
          .groupBy("PULocationID")
          .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare")))
print(f"  three transformations took   {time.time()-t0:7.3f} s")

print("  RUNNING the query")
t0 = time.time()
n = busy.count()
print(f"  one action took              {time.time()-t0:7.3f} s   and returned {n} rows")
print()

# ---- the plan -------------------------------------------------------------
print("  ---- physical plan ----")
busy.explain(mode="formatted")

spark.stop()
