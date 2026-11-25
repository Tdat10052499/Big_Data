"""Lab 3, Exercise 3: Section 7's windowed count on taxi-trips, 1-minute and 5-minute windows.

No watermark, trigger(availableNow=True), outputMode("complete"), memory sink.
Run:  $SS src/ex03_windows.py
"""
from pyspark.sql import functions as F
from s0_common import spark_session, trip_stream, TOPIC

spark = spark_session("lab03-ex03-windows")

for size, qname in [("1 minute", "win_1min"), ("5 minutes", "win_5min")]:
    trips = trip_stream(spark)
    counted = (trips.groupBy(F.window("event_time", size))
                    .agg(F.count("*").alias("trips"),
                         F.min("event_time").alias("min_et"),
                         F.max("event_time").alias("max_et")))
    q = (counted.writeStream
         .outputMode("complete")
         .format("memory")
         .queryName(qname)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()

    rows = spark.sql(f"SELECT window.start AS start, window.end AS end, trips, min_et, max_et "
                     f"FROM {qname} ORDER BY window.start").collect()
    total = sum(r["trips"] for r in rows)
    print()
    print(f"  window size {size}, topic {TOPIC}, no watermark, availableNow, complete mode")
    print(f"  number of {size} windows with trips from topic {TOPIC}: {len(rows)}")
    print(f"  {'start':<21} {'end':<21} {'trips':>6}")
    for r in rows:
        print(f"  {str(r['start']):<21} {str(r['end']):<21} {r['trips']:>6,}   topic {TOPIC}")
    print(f"  sum of trips over all {size} windows, topic {TOPIC}: {total:,}"
          f"   check equals 20,000: {'PASS' if total == 20000 else 'FAIL'}")
    print(f"  min event_time seen in topic {TOPIC}: {min(r['min_et'] for r in rows)}")
    print(f"  max event_time seen in topic {TOPIC}: {max(r['max_et'] for r in rows)}")
print()
spark.stop()
