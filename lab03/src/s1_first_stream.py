"""Lab 3, Section 5: the first streaming query.

Reads every event currently in the topic, counts trips per pickup zone, and stops.
Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
          ~/bda/lab03/src/s1_first_stream.py
"""
from pyspark.sql import functions as F
from s0_common import spark_session, trip_stream

spark = spark_session("lab03-first-stream")
trips = trip_stream(spark)

print()
print("  is this a streaming DataFrame?", trips.isStreaming)
print()

counts = trips.groupBy("pickup_zone").agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"))

# availableNow processes everything already in the topic, then stops on its own.
# The code is the same as a query that runs forever. Only the trigger differs,
# which makes the result repeatable while you are learning.
query = (counts.writeStream
         .outputMode("complete")
         .format("memory")
         .queryName("zone_counts")
         .trigger(availableNow=True)
         .start())

query.awaitTermination()

result = spark.sql("SELECT * FROM zone_counts ORDER BY trips DESC")
total = spark.sql("SELECT sum(trips) AS n FROM zone_counts").collect()[0]["n"]

print(f"  events read from the topic: {total:,}")
print(f"  distinct pickup zones:      {result.count()}")
print()
result.show(8, truncate=False)

spark.stop()
