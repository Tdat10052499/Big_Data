"""Exercises 2 and 3: profile March; null pattern in February."""
from functools import reduce
import operator
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab01-ex02-03").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
feb = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-02.parquet")
mar = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-03.parquet")

print("\n=== Exercise 2: March ===")
print(f"rows    {mar.count():,}")
print(f"columns {len(mar.columns)}")
jd, md = dict(jan.dtypes), dict(mar.dtypes)
print("same column names and order:", jan.columns == mar.columns)
diffs = [(c, jd.get(c), md.get(c)) for c in sorted(set(jd) | set(md)) if jd.get(c) != md.get(c)]
print("columns that differ in name or type (column, jan, mar):", diffs if diffs else "none")

FIVE = ["passenger_count", "RatecodeID", "store_and_fwd_flag", "congestion_surcharge", "Airport_fee"]

def pattern(df, label):
    total = df.count()
    print(f"\n=== Exercise 3: {label}, cohort = {total:,} rows of that month ===")
    nulls = df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    for c, n in sorted(nulls.items(), key=lambda kv: -kv[1]):
        if n > 0:
            print(f"  {c:<24} {n:>9,}  {n/total:6.3%} of {label} rows")
    allfive = reduce(operator.and_, [F.col(c).isNull() for c in FIVE])
    pay0 = F.col("payment_type") == 0
    print(f"  rows where ALL FIVE are null:              {df.filter(allfive).count():,}")
    print(f"  rows with payment_type = 0:                {df.filter(pay0).count():,}")
    print(f"  all five null AND payment_type = 0:        {df.filter(allfive & pay0).count():,}")
    print(f"  all five null AND payment_type != 0:       {df.filter(allfive & ~pay0).count():,}")
    print(f"  payment_type = 0 AND NOT all five null:    {df.filter(pay0 & ~allfive).count():,}")

pattern(feb, "February")
pattern(jan, "January (for comparison)")
spark.stop()
