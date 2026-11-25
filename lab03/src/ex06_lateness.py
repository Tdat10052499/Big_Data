"""Lab 3, Exercise 6: how late are the events in taxi-trips (sent_at minus event_time)?

Batch read of the whole topic (earliest to latest), millisecond-precision lateness,
exact percentiles, and two estimates of what candidate watermarks would discard.
Run:  $SS src/ex06_lateness.py
"""
from pyspark.sql import functions as F
from s0_common import spark_session, SCHEMA, BOOTSTRAP

TOPIC = "taxi-trips"
spark = spark_session("lab03-ex06-lateness")

raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("endingOffsets", "latest")
       .load())

ev = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"),
                 "timestamp", "partition", "offset")
         .select("t.*", "timestamp", "partition", "offset")
         .withColumn("event_time", F.to_timestamp("event_time"))
         .withColumn("sent_at", F.to_timestamp("sent_at"))
         .withColumn("lateness_s", F.col("sent_at").cast("double") - F.col("event_time").cast("double"))
         .cache())

N = ev.count()
COH = f"of the {N:,} events in topic {TOPIC}"


def stats(df, label):
    r = df.agg(F.count("*").alias("n"), F.min("lateness_s").alias("mn"),
               F.expr("percentile(lateness_s, array(0.5, 0.95, 0.99))").alias("p"),
               F.max("lateness_s").alias("mx")).collect()[0]
    print(f"  {label}: count {r['n']:,}  ({r['n'] / N:.2%} {COH})")
    print(f"    min lateness_s     {r['mn']:.3f}   topic {TOPIC}, {label}")
    print(f"    median lateness_s  {r['p'][0]:.3f}   topic {TOPIC}, {label}")
    print(f"    p95 lateness_s     {r['p'][1]:.3f}   topic {TOPIC}, {label}")
    print(f"    p99 lateness_s     {r['p'][2]:.3f}   topic {TOPIC}, {label}")
    print(f"    max lateness_s     {r['mx']:.3f}   topic {TOPIC}, {label}")


print()
print(f"  lateness_s = cast(sent_at as double) - cast(event_time as double), millisecond precision")
print(f"  events read from topic {TOPIC}: {N:,}")
print(f"  percentiles are exact (Spark SQL percentile, not percentile_approx)")
print()
stats(ev, f"all events {COH}")
stats(ev.filter("is_late = true"), f"is_late = true, {COH}")
stats(ev.filter("is_late = false"), f"is_late = false, {COH}")

# Arrival order: Kafka timestamp, then partition, then offset.
rows = (ev.select(F.col("timestamp").cast("double").alias("ts"), "partition", "offset",
                  F.col("event_time").cast("double").alias("et"), "lateness_s")
          .collect())
rows.sort(key=lambda r: (r["ts"], r["partition"], r["offset"]))

print()
print(f"  candidate watermarks, share of events each would discard, as % {COH}")
print(f"  (a) simple: events with lateness_s > W")
print(f"  (b) arrival-order: events in Kafka timestamp, partition, offset order; an event is discarded")
print(f"      if its event_time < (running max event_time of the events before it) - W.")
print(f"      This ignores Spark's per-batch watermark granularity (Spark advances the watermark")
print(f"      only between micro-batches), so it is an estimate, not what a query would report.")
print(f"  {'W':>8} {'(a) events':>11} {'(a) share':>10} {'(b) events':>11} {'(b) share':>10}")
for w in [1, 2, 5, 10, 11, 12, 15]:
    ws = w * 60
    a = sum(1 for r in rows if r["lateness_s"] > ws)
    b, mx = 0, None
    for r in rows:
        if mx is not None and r["et"] < mx - ws:
            b += 1
        mx = r["et"] if mx is None else max(mx, r["et"])
    print(f"  {w:>4} min {a:>11,} {a / N:>9.2%} {b:>11,} {b / N:>9.2%}   {COH}")
print()
spark.stop()
