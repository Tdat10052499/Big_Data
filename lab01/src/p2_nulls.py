"""Lab 1, Section 6.3 and 6.4: counting what is missing, then proving it is
one population rather than five.
Run:  spark-submit ~/bda/lab01/src/p2_nulls.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab01-nulls").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
TOTAL = jan.count()

nulls = jan.select([
    F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in jan.columns
]).collect()[0].asDict()

print()
print("Columns with missing values:")
print()
for col, n in sorted(nulls.items(), key=lambda kv: -kv[1]):
    if n > 0:
        print(f"  {col:<24} {n:>9,}  {n/TOTAL:6.2%}")

same = jan.filter(
    F.col("passenger_count").isNull() &
    F.col("RatecodeID").isNull() &
    F.col("store_and_fwd_flag").isNull() &
    F.col("congestion_surcharge").isNull() &
    F.col("Airport_fee").isNull()
).count()

print()
print(f"  rows where ALL FIVE are null: {same:,}")
print()
jan.groupBy("payment_type").agg(F.count("*").alias("rows")).orderBy("payment_type").show()

spark.stop()
