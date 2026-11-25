"""Lab 3, Exercise 2: the first streaming query, grouped by dropoff zone.

Reads every event currently in the topic, counts trips per dropoff zone, and stops.
Run:  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
          ~/bda/lab03/src/ex02_dropoff.py
"""
from pyspark.sql import functions as F
from s0_common import spark_session, trip_stream, TOPIC

spark = spark_session("lab03-ex02-dropoff")
trips = trip_stream(spark)

print()
print("  is this a streaming DataFrame?", trips.isStreaming)
print()

counts = trips.groupBy("dropoff_zone").agg(
    F.count("*").alias("trips"),
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"))

# availableNow processes everything already in the topic, then stops on its own.
# The code is the same as a query that runs forever. Only the trigger differs,
# which makes the result repeatable while you are learning.
query = (counts.writeStream
         .outputMode("complete")
         .format("memory")
         .queryName("dropoff_counts")
         .trigger(availableNow=True)
         .start())

query.awaitTermination()

result = spark.sql("SELECT * FROM dropoff_counts ORDER BY trips DESC, dropoff_zone")
total = spark.sql("SELECT sum(trips) AS n FROM dropoff_counts").collect()[0]["n"]

print("  top five dropoff zones:")
for r in result.limit(5).collect():
    print(f"    dropoff_zone {r['dropoff_zone']:>4}  trips from topic {TOPIC}: {r['trips']:,}  avg_fare {r['avg_fare']}")
print(f"  distinct dropoff zones in topic {TOPIC}: {result.count()}")
print(f"  events read from topic {TOPIC}: {total:,}")
print()
result.show(8, truncate=False)

spark.stop()
