"""Lab 3, Section 11: how fast is the stream, and how far behind is it.

Two different questions, and students mix them up.

  Throughput  how many events per second the query gets through.
  Latency     how old an event is by the time the query handles it.

Throughput comes from the query's own progress report. Latency has to be
computed from the data, by comparing the time of the event against the clock at
the moment the batch runs.

Start the producer first, in another terminal:
    python src/trip_producer.py --topic taxi-live --rate 1500 --limit 60000

Then:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
        --py-files src/s0_common.py src/s4_latency.py
"""
import subprocess
import sys
import time

from pyspark.sql import functions as F
from s0_common import spark_session, SCHEMA, BOOTSTRAP

TOPIC = "taxi-live"
STREAM = "hdfs://localhost:9000/user/thaianh/nyc/stream"
CKPT = f"{STREAM}/ck/latency"
BATCHES = 8

subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", CKPT], capture_output=True)

spark = spark_session("lab03-latency")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest")
       .load())

trips = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"))
            .select("t.*")
            .withColumn("event_time", F.to_timestamp("event_time")))

lags = {}


def measure(df, batch_id):
    """Lag is measured against the clock now, inside the batch, so it includes
    the time the event waited in Kafka as well as the time Spark took."""
    row = (df.withColumn("lag_s", F.unix_timestamp(F.current_timestamp())
                         - F.unix_timestamp("event_time"))
             .agg(F.count("*").alias("n"),
                  F.round(F.avg("lag_s"), 1).alias("avg_lag"),
                  F.round(F.max("lag_s"), 1).alias("max_lag"))
             .collect()[0])
    if row["n"]:
        lags[batch_id] = (row["n"], row["avg_lag"], row["max_lag"])


q = (trips.writeStream
     .outputMode("append")
     .foreachBatch(measure)
     .option("checkpointLocation", CKPT)
     .trigger(processingTime="3 seconds")
     .start())

print()
print(f"  watching {BATCHES} batches at a 3 second trigger")
while len([p for p in q.recentProgress if p.get("numInputRows", 0) > 0]) < BATCHES:
    if not q.isActive:
        break
    time.sleep(2)
q.stop()

print()
print(f"  {'batch':>6} {'rows':>7} {'rows/s in':>10} {'rows/s out':>11} "
      f"{'trigger ms':>11} {'avg lag s':>10} {'max lag s':>10}")
for p in q.recentProgress:
    n = p.get("numInputRows", 0)
    if not n:
        continue
    bid = p.get("batchId")
    lag = lags.get(bid, (n, None, None))
    print(f"  {bid:>6} {n:>7,} {p.get('inputRowsPerSecond') or 0:>10.0f} "
          f"{p.get('processedRowsPerSecond') or 0:>11.0f} "
          f"{p.get('durationMs', {}).get('triggerExecution', 0):>11,} "
          f"{lag[1] if lag[1] is not None else 0:>10} {lag[2] if lag[2] is not None else 0:>10}")
print()

spark.stop()
