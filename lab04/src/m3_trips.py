"""Lab 4, Section 9: put one month of trips into MongoDB, the same rows Parquet already holds.

    $SS src/m3_trips.py

Writes the collection bda.trips_jan, replacing it if it exists, then reports
how long the load took and how much room the same rows take in each store.
"""
import time

from pymongo import MongoClient
from pyspark.sql import functions as F
from l0_common import spark_session, NYC, FMT, hdfs_bytes, mb

spark = spark_session("lab04-mongo-trips")
jan = spark.read.parquet(f"{NYC}/curated")                  # 2,724,143 rows, Lab 1

# MongoDB has one type for a date and time, and it is always in UTC. The trip
# times are New York clock times with no zone attached, so convert them as if
# they were UTC. Then 08:00 stays 08:00 in MongoDB, whatever time zone the
# laptop running this script is set to.
spark.conf.set("spark.sql.session.timeZone", "UTC")
docs =(jan.withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast("timestamp"))
           .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast("timestamp"))
           .withColumn("pickup_date", F.col("pickup_date").cast("timestamp")))

t0 = time.time()
(docs.write.format("mongodb").mode("overwrite")
     .option("database", "bda").option("collection", "trips_jan").save())
load_s = time.time() - t0

stats = MongoClient("mongodb://127.0.0.1:27017")["bda"].command("collStats", "trips_jan")
parquet = hdfs_bytes(f"{FMT}/2024-01/parquet")

print()
print(f"  documents written          {stats['count']:,}")
print(f"  load time                  {load_s:.1f} s")
print()
print(f"  {'the same rows, stored as':30}{'bytes':>10}{'against Parquet':>18}")
print(f"  {'Parquet on HDFS':30}{mb(parquet):>10}{'1.0x':>18}")
print(f"  {'MongoDB, documents on disk':30}{mb(stats['storageSize']):>10}"
      f"{stats['storageSize'] / parquet:>17.1f}x")
print(f"  {'MongoDB, documents unpacked':30}{mb(stats['size']):>10}"
      f"{stats['size'] / parquet:>17.1f}x")
print(f"  {'MongoDB, indexes on disk':30}{mb(stats['totalIndexSize']):>10}")
print()
spark.stop()
