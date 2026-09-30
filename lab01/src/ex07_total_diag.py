"""Exercise 7 follow-up: what is the total_amount mismatch?"""
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("lab01-ex07b").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")
c, zero = F.col, F.lit(0.0)
cong = F.coalesce(c("congestion_surcharge"), zero)
parts = (c("fare_amount") + c("extra") + c("mta_tax") + c("tip_amount") + c("tolls_amount")
         + c("improvement_surcharge") + cong + F.coalesce(c("Airport_fee"), zero))
d = (c("total_amount") - parts)
df = (jan.withColumn("diff", F.round(d, 2))
         .withColumn("mism", (F.abs(d) > 0.01).cast("int"))
         .withColumn("eq_cong", ((F.abs(d) > 0.01) & (F.abs(d + cong) <= 0.01)).cast("int")))
TOTAL = df.count()
n = df.agg(F.sum("mism"), F.sum("eq_cong")).collect()[0]
print(f"\nJanuary rows: {TOTAL:,}")
print(f"Mismatched rows: {n[0]:,} ({n[0] / TOTAL:.3%} of the {TOTAL:,} January rows)")
print(f"Of those, rows where diff equals minus congestion_surcharge (+-0.01): {n[1]:,} ({n[1] / n[0]:.3%} of the mismatched rows)")

print("\nMost common diff values among mismatched rows:")
df.filter("mism = 1").groupBy("diff").count().orderBy(F.desc("count")).show(10)

def by(col, label):
    print(f"Mismatch share by {label} (cohort = rows with that value):")
    (df.groupBy(col).agg(F.count("*").alias("rows"), F.sum("mism").alias("mismatched"))
       .withColumn("share", F.round(c("mismatched") / c("rows") * 100, 3)).orderBy(col).show())

by("congestion_surcharge", "congestion_surcharge")
by("VendorID", "VendorID")
by("payment_type", "payment_type")
spark.stop()
