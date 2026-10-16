"""Lab 2, Sections 8 and 9: join strategy, and when caching pays.
Run:  spark-submit ~/bda/lab02/src/q4_join_cache.py
"""
import time
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("lab02-join-cache").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

HDFS = "hdfs://localhost:9000"
q1 = spark.read.parquet(f"{HDFS}/user/tdat1/nyc/curated_q1")
zones = (spark.read.option("header", True)
         .csv(f"{HDFS}/user/tdat1/nyc/ref/taxi_zone_lookup.csv")
         .withColumn("LocationID", F.col("LocationID").cast("int")))


def best_of_three(fn):
    fn()
    out = []
    for _ in range(3):
        t0 = time.time(); fn(); out.append(time.time() - t0)
    return min(out)


# ===================================================== join strategy
def join_by_borough(broadcast):
    z = F.broadcast(zones) if broadcast else zones
    return (q1.join(z, q1.PULocationID == z.LocationID, "left")
              .groupBy("Borough").agg(F.count("*").alias("trips")).collect())


print()
print(f"  trips {q1.count():,}   zones {zones.count()}")
print()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # forbid broadcasting
smj = best_of_three(lambda: join_by_borough(False))
print(f"  sort merge join      {smj:6.2f} s")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
bhj = best_of_three(lambda: join_by_borough(True))
print(f"  broadcast hash join  {bhj:6.2f} s      {smj/bhj:.1f} times faster")
print()

print("  ---- the operator each one chose ----")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
p1 = q1.join(zones, q1.PULocationID == zones.LocationID, "left")
for line in p1._jdf.queryExecution().executedPlan().toString().splitlines()[:4]:
    if line.strip():
        print("  " + line[:92])
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
p2 = q1.join(F.broadcast(zones), q1.PULocationID == zones.LocationID, "left")
print()
for line in p2._jdf.queryExecution().executedPlan().toString().splitlines()[:4]:
    if line.strip():
        print("  " + line[:92])
print()

# ===================================================== caching
long_trips = q1.filter(F.col("trip_distance") > 2)


def one_pass(df):
    return lambda: df.agg(F.avg("fare_amount")).collect()


def five_passes(df):
    def go():
        df.agg(F.avg("fare_amount")).collect()
        df.groupBy("PULocationID").count().collect()
        df.agg(F.max("trip_distance")).collect()
        df.groupBy("pickup_hour").agg(F.avg("trip_minutes")).collect()
        df.filter(F.col("fare_amount") > 50).count()
    return go


print(f"  rows being reused    {long_trips.count():,}")
print()
u1 = best_of_three(one_pass(long_trips))
u5 = best_of_three(five_passes(long_trips))
long_trips.cache(); long_trips.count()
c1 = best_of_three(one_pass(long_trips))
c5 = best_of_three(five_passes(long_trips))
long_trips.unpersist()

print(f"  {'':<14}{'uncached':>10}{'cached':>10}{'change':>12}")
print(f"  {'one pass':<14}{u1:>9.2f}s{c1:>9.2f}s{u1/c1:>11.1f}x")
print(f"  {'five passes':<14}{u5:>9.2f}s{c5:>9.2f}s{u5/c5:>11.1f}x")
print()

spark.stop()
