"""Exercise 9, part 2: how long does Spark take to count the rows of the three months already in HDFS?"""
import time
from pyspark.sql import SparkSession, functions as F

t0 = time.perf_counter()
spark = SparkSession.builder.appName("lab01-ex09-count").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
t_session = time.perf_counter() - t0
RAW = "hdfs://localhost:9000/user/tdat1/nyc/raw"
paths = [f"{RAW}/yellow_tripdata_2024-0{m}.parquet" for m in (1, 2, 3)]

t = time.perf_counter()
df = spark.read.parquet(*paths)
t_read = time.perf_counter() - t
print(f"RESULT session start {t_session:.3f} s; spark.read.parquet (schema only) {t_read:.3f} s")
for k in (1, 2):
    t = time.perf_counter()
    n = df.count()
    print(f"RESULT count #{k}: {n:,} rows in {time.perf_counter() - t:.3f} s (inside the process)")
t = time.perf_counter()
s = df.agg(F.sum("fare_amount")).collect()[0][0]
print(f"RESULT sum(fare_amount) = {s:,.2f} in {time.perf_counter() - t:.3f} s (forces reading one column)")
spark.stop()
