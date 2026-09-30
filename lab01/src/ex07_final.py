"""Exercise 7 final check: total_amount vs the sum of its components, without assuming null = 0."""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab01-ex07-final").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
c = F.col
TOTAL = jan.count()
parts_nn = (c("fare_amount") + c("extra") + c("mta_tax") + c("tip_amount") + c("tolls_amount")
            + c("improvement_surcharge") + c("congestion_surcharge") + c("Airport_fee"))
coh = jan.filter(c("congestion_surcharge").isNotNull() & c("Airport_fee").isNotNull())
df = (coh.withColumn("d", c("total_amount") - parts_nn)
         .withColumn("diff", F.round(c("d"), 2))
         .withColumn("mism", (F.abs(c("d")) > 0.01).cast("int"))
         .withColumn("eq_cong", ((F.abs(c("d")) > 0.01) & (F.abs(c("d") + c("congestion_surcharge")) <= 0.01)).cast("int")))
N = df.count()
r = df.agg(F.sum("mism"), F.sum("eq_cong"),
           F.sum(F.when(c("mism") == 1, c("d")).otherwise(0.0)),
           F.sum(F.when(c("mism") == 1, F.abs(c("d"))).otherwise(0.0))).collect()[0]
m = r[0]
print(f"\nCohort: rows with non-null congestion_surcharge and Airport_fee = {N:,} (of the {TOTAL:,} January rows)")
print(f"total_amount differs from the sum of its 8 components by more than 0.01: {m:,} = {m / N:.3%} of the {N:,} cohort rows")
print(f"  of those, diff equals minus congestion_surcharge (+-0.01): {r[1]:,} = {r[1] / m:.3%} of the {m:,} mismatched rows")
print(f"  net sum of (total_amount - components) over mismatched rows: {r[2]:,.2f} USD; sum of absolute differences: {r[3]:,.2f} USD")
print("\nMost common diff values among mismatched cohort rows:")
df.filter("mism = 1").groupBy("diff").count().orderBy(F.desc("count")).show(6)
print("Mismatch share by VendorID (cohort = cohort rows with that VendorID):")
(df.groupBy("VendorID").agg(F.count("*").alias("rows"), F.sum("mism").alias("mismatched"),
                            F.sum("eq_cong").alias("diff_eq_minus_cong"))
   .withColumn("share_pct", F.round(c("mismatched") / c("rows") * 100, 3)).orderBy("VendorID").show())

print("Rows with null congestion_surcharge (and null Airport_fee): diff when null is treated as 0")
nul = jan.filter(c("congestion_surcharge").isNull() & c("Airport_fee").isNull())
print(f"  rows: {nul.count():,}")
(nul.withColumn("diff", F.round(c("total_amount") - (c("fare_amount") + c("extra") + c("mta_tax") + c("tip_amount")
                     + c("tolls_amount") + c("improvement_surcharge")), 2))
    .groupBy("diff").count().orderBy(F.desc("count")).show(5))
spark.stop()
