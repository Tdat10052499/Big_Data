from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("lab01-ex04").master("local[4]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
H = "hdfs://localhost:9000/user/tdat1/nyc"
a = spark.read.parquet(f"{H}/curated").count()
b = spark.read.parquet(f"{H}/curated_copy").count()
print(f"curated rows {a:,}; curated_copy rows {b:,}; equal: {a == b}")
spark.stop()
