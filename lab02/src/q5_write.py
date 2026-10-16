"""Lab 2, Section 10: write the three artefacts Lab 4 will read, and look at
how many files each write produced.
Run:  spark-submit ~/bda/lab02/src/q5_write.py
"""
import subprocess, time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab02-write").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
AGG = f"{HDFS}/user/tdat1/nyc/agg"
q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")
zones = (spark.read.option("header", True)
         .csv(f"{HDFS}/user/tdat1/nyc/ref/taxi_zone_lookup.csv")
         .withColumn("LocationID", F.col("LocationID").cast("int")))


def files_in(path):
    out = subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-ls", "-R", path],
                         capture_output=True, text=True).stdout
    return sum(1 for l in out.splitlines() if l.startswith("-") and "_SUCCESS" not in l)


print()

# --- artefact 1: one row per zone ------------------------------------------
zone_summary = (q1.groupBy("PULocationID")
                  .agg(F.count("*").alias("trips"),
                       F.avg("fare_amount").alias("avg_fare"),
                       F.avg("trip_distance").alias("avg_distance"))
                  .withColumnRenamed("PULocationID", "zone_id"))
zone_summary.write.mode("overwrite").parquet(f"{AGG}/zone_summary")
print(f"  zone_summary   {zone_summary.count():>7,} rows   {files_in(f'{AGG}/zone_summary')} file(s)")

# --- artefact 2: one row per zone and hour ---------------------------------
zone_hour = (q1.groupBy("PULocationID", "pickup_hour")
               .agg(F.avg("fare_amount").alias("zone_hour_avg")))
zone_hour.write.mode("overwrite").parquet(f"{AGG}/zone_hour")
print(f"  zone_hour      {zone_hour.count():>7,} rows   {files_in(f'{AGG}/zone_hour')} file(s)")

# --- artefact 3: the trips, split by date ----------------------------------
t0 = time.time()
q1.write.mode("overwrite").partitionBy("pickup_date").parquet(f"{AGG}/q1_by_date")
naive_s, naive_f = time.time() - t0, files_in(f"{AGG}/q1_by_date")

t0 = time.time()
(q1.repartition("pickup_date").write.mode("overwrite")
   .partitionBy("pickup_date").parquet(f"{AGG}/q1_by_date_repart"))
repart_s, repart_f = time.time() - t0, files_in(f"{AGG}/q1_by_date_repart")

dates = q1.select("pickup_date").distinct().count()
print()
print(f"  dates in the data                     {dates}")
print(f"  partitionBy on its own        {naive_s:6.1f} s   {naive_f} files")
print(f"  repartition first             {repart_s:6.1f} s   {repart_f} files")
print()

# --- does splitting by date make a date query faster? ----------------------
by_date = spark.read.parquet(f"{AGG}/q1_by_date")


def best(fn):
    """Warm up once, then return the fastest of three runs."""
    fn()
    runs = []
    for _ in range(3):
        t0 = time.time()
        fn()
        runs.append(time.time() - t0)
    return min(runs)


def all_days():   return by_date.agg(F.sum("fare_amount")).collect()
def one_day():    return by_date.filter(F.col("pickup_date") == "2024-02-14").agg(F.sum("fare_amount")).collect()
def one_day_flat(): return q1.filter(F.col("pickup_date") == "2024-02-14").agg(F.sum("fare_amount")).collect()


print(f"  sum over all {dates} days, split by date      {best(all_days):6.2f} s")
print(f"  sum over one day, split by date             {best(one_day):6.2f} s")
print(f"  sum over one day, NOT split by date         {best(one_day_flat):6.2f} s")
print()

spark.stop()
