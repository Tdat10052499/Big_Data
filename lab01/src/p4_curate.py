"""Lab 1, Section 9: produce the curated dataset that Labs 2, 3 and 4 consume.
Writes hdfs://localhost:9000/user/thaianh/nyc/curated  (January 2024 only).
Run:  spark-submit ~/bda/lab01/src/p4_curate.py
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab01-curate").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
jan = spark.read.parquet(f"{HDFS}/user/thaianh/nyc/raw/yellow_tripdata_2024-01.parquet")
RAW_ROWS = jan.count()

curated = (jan
  .filter((F.col("fare_amount") >= 0)
        & (F.col("trip_distance") > 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("tpep_pickup_datetime") >= "2024-01-01")
        & (F.col("tpep_pickup_datetime") <  "2024-02-01")
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")))
  .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
  .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
  # timestamp_ntz will not cast straight to a number, so cast through timestamp
  .withColumn("trip_minutes",
      (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
     - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0))

curated.write.mode("overwrite").parquet(f"{HDFS}/user/thaianh/nyc/curated")

kept = spark.read.parquet(f"{HDFS}/user/thaianh/nyc/curated").count()
dropped = RAW_ROWS - kept

print()
print(f"  raw     {RAW_ROWS:,}")
print(f"  kept    {kept:,}")
print(f"  dropped {dropped:,}")
print(f"  share   {dropped/RAW_ROWS:.2%} of the {RAW_ROWS:,} January rows")
print()

spark.stop()
