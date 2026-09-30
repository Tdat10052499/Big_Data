"""Lab 1, Section 6.5 and 7: observed ranges, then the quality checks.
Writes out/quality_report.csv, which is a required checkpoint artefact.
Run:  spark-submit ~/bda/lab01/src/p3_defects.py
"""
import csv, os
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab01-defects").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
TOTAL = jan.count()

print()
print("Observed ranges")
jan.select(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.min("fare_amount").alias("min_fare"),
    F.max("fare_amount").alias("max_fare"),
    F.min("trip_distance").alias("min_dist"),
    F.max("trip_distance").alias("max_dist"),
).show(truncate=False)

checks = {
    "fare_amount below zero":          F.col("fare_amount") < 0,
    "fare_amount exactly zero":        F.col("fare_amount") == 0,
    "total_amount below zero":         F.col("total_amount") < 0,
    "passenger_count is zero":         F.col("passenger_count") == 0,
    "passenger_count is null":         F.col("passenger_count").isNull(),
    "trip_distance is zero":           F.col("trip_distance") == 0,
    "zero distance but fare above 0": (F.col("trip_distance") == 0) & (F.col("fare_amount") > 0),
    "trip_distance above 100 miles":   F.col("trip_distance") > 100,
    "pickup outside January 2024":    (F.col("tpep_pickup_datetime") < "2024-01-01") |
                                      (F.col("tpep_pickup_datetime") >= "2024-02-01"),
    "dropoff before pickup":           F.col("tpep_dropoff_datetime") < F.col("tpep_pickup_datetime"),
    "dropoff equal to pickup":         F.col("tpep_dropoff_datetime") == F.col("tpep_pickup_datetime"),
    "unknown pickup zone 264 or 265":  F.col("PULocationID").isin(264, 265),
}

print(f"Quality checks. Every share is of the {TOTAL:,} January rows.")
print()
rows = []
for name, condition in checks.items():
    n = jan.filter(condition).count()
    print(f"  {name:<32} {n:>8,}  {n/TOTAL:7.3%}")
    rows.append({"check": name, "rows": n, "share_of_january": f"{n/TOTAL:.3%}"})

dupes = TOTAL - jan.dropDuplicates().count()
print(f"  {'exact duplicate rows':<32} {dupes:>8,}  {dupes/TOTAL:7.3%}")
rows.append({"check": "exact duplicate rows", "rows": dupes,
             "share_of_january": f"{dupes/TOTAL:.3%}"})

out = os.path.expanduser("~/bda/lab01/out/quality_report.csv")
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["check", "rows", "share_of_january"])
    w.writeheader()
    w.writerows(rows)
print()
print(f"  cohort size (denominator): {TOTAL:,}")
print(f"  written: {out}")

spark.stop()
