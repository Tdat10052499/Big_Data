"""Lab 2, Section 3: apply the Lab 1 curation rules to all three months.
Writes hdfs://localhost:9000/user/tdat1/nyc/curated_q1
Run:  spark-submit ~/bda/lab02/src/q1_scale.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab02-scale-up").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"

# Reading the directory reads all three files at once. No loop is needed.
raw = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/raw")
print()
print(f"  raw rows, all three months   {raw.count():,}")

curated = (raw
  .filter((F.col("fare_amount") >= 0)
        & (F.col("trip_distance") > 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("tpep_pickup_datetime") >= "2024-01-01")
        & (F.col("tpep_pickup_datetime") <  "2024-04-01")
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")))
  .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
  .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
  .withColumn("trip_minutes",
      (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
     - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0))

curated.write.mode("overwrite").parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")

q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")
kept = q1.count()
raw_rows = raw.count()
print(f"  curated_q1 rows              {kept:,}")
print(f"  dropped                      {raw_rows - kept:,}")
print(f"  share dropped                {(raw_rows-kept)/raw_rows:.2%} of the {raw_rows:,} raw rows")
print(f"  partitions in memory         {q1.rdd.getNumPartitions()}")
print()

spark.stop()
