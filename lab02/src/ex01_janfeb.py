"""Lab 2, Exercise 1: apply the Lab 1 curation rules to January and February only.
Reads   hdfs://localhost:9000/user/tdat1/nyc/raw/yellow_tripdata_2024-0{1,2}.parquet
Writes  hdfs://localhost:9000/user/tdat1/nyc/curated_jan_feb
Run:    spark-submit ~/bda/lab02/src/ex01_janfeb.py
The rules are those of q1_scale.py; only the input files and the upper date bound change.
"""
from pyspark.sql import SparkSession, functions as F
from common import RAW, CURATED_JAN_FEB

spark = (SparkSession.builder.appName("lab02-ex01-janfeb").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

FILES = [f"{RAW}/yellow_tripdata_2024-01.parquet",
         f"{RAW}/yellow_tripdata_2024-02.parquet"]
LOW, HIGH = "2024-01-01", "2024-03-01"   # pickup in [LOW, HIGH)

per_file = [spark.read.parquet(p).count() for p in FILES]
raw = spark.read.parquet(*FILES)
raw_rows = raw.count()

curated = (raw
  .filter((F.col("fare_amount") >= 0)
        & (F.col("trip_distance") > 0)
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & (F.col("tpep_pickup_datetime") >= LOW)
        & (F.col("tpep_pickup_datetime") <  HIGH)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")))
  .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
  .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
  .withColumn("trip_minutes",
      (F.col("tpep_dropoff_datetime").cast("timestamp").cast("long")
     - F.col("tpep_pickup_datetime").cast("timestamp").cast("long")) / 60.0))

curated.write.mode("overwrite").parquet(CURATED_JAN_FEB)

out = spark.read.parquet(CURATED_JAN_FEB)
kept = out.count()
dropped = raw_rows - kept
span = out.agg(F.min("pickup_date").alias("lo"), F.max("pickup_date").alias("hi")).collect()[0]

print()
print(f"  window                       pickup in [{LOW}, {HIGH})")
print(f"  raw rows, 2024-01 file       {per_file[0]:,}")
print(f"  raw rows, 2024-02 file       {per_file[1]:,}")
print(f"  raw rows, Jan+Feb            {raw_rows:,}")
print(f"  curated_jan_feb rows (kept)  {kept:,}")
print(f"  dropped                      {dropped:,}")
print(f"  share dropped                {dropped/raw_rows:.2%} of the {raw_rows:,} raw Jan+Feb rows")
print(f"  pickup_date range kept       {span['lo']} .. {span['hi']}")
print(f"  partitions in memory         {out.rdd.getNumPartitions()}")
print()

spark.stop()
