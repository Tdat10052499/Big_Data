"""Lab 1, Section 6.2: the first look at the data.
Run:  spark-submit ~/bda/lab01/src/p1_schema.py
"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("lab01-profile")
         .master("local[4]")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

RAW = "hdfs://localhost:9000/user/thaianh/nyc/raw"
jan = spark.read.parquet(f"{RAW}/yellow_tripdata_2024-01.parquet")

print()
print("rows   ", f"{jan.count():,}")
print("columns", len(jan.columns))
print()
jan.printSchema()

spark.stop()
