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
import csv
import subprocess
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F
from s0_common import spark_session, SCHEMA, BOOTSTRAP



def _opt(name, default):
    """--name value, read from the command line (tdat1 edit)."""
    if name in sys.argv:
        return sys.argv[sys.argv.index(name) + 1]
    return default


TOPIC = _opt("--topic", "taxi-live")
TRIGGER = int(_opt("--trigger", "3"))
TAG = _opt("--tag", "sec11")
STREAM = "hdfs://localhost:9000/user/tdat1/nyc/stream"
CKPT = f"{STREAM}/ck/latency_{TAG}"
BATCHES = int(_opt("--batches", "8"))
PARTS = Path(__file__).resolve().parent.parent / "out" / "latency_parts"
STARTING = _opt("--starting", "latest")
MAX_OFFSETS = _opt("--max-offsets", None)

print(f"  topic {TOPIC}")
print(f"  trigger {TRIGGER} seconds")
print(f"  checkpoint {CKPT}")
print(f"  startingOffsets {STARTING}")
print(f"  maxOffsetsPerTrigger {MAX_OFFSETS if MAX_OFFSETS is not None else 'none'}", flush=True)

subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", CKPT], capture_output=True)

spark = spark_session("lab03-latency")

reader = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", BOOTSTRAP)
          .option("subscribe", TOPIC)
          .option("startingOffsets", STARTING))
if MAX_OFFSETS is not None:
    reader = reader.option("maxOffsetsPerTrigger", int(MAX_OFFSETS))
raw = reader.load()

trips = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"))
            .select("t.*")
            .withColumn("event_time", F.to_timestamp("event_time")))

lags = {}


def measure(df, batch_id):
    """Lag is measured against the clock now, inside the batch, so it includes
    the time the event waited in Kafka as well as the time Spark took."""
    row = (df.withColumn("lag_s", F.unix_timestamp(F.current_timestamp())
                         - F.unix_timestamp("event_time"))
             .withColumn("lag_ms_prec", F.current_timestamp().cast("double")
                         - F.col("event_time").cast("double"))
             .agg(F.count("*").alias("n"),
                  F.round(F.avg("lag_s"), 1).alias("avg_lag"),
                  F.round(F.max("lag_s"), 1).alias("max_lag"),
                  F.avg("lag_ms_prec").alias("avg_lag_p"),
                  F.max("lag_ms_prec").alias("max_lag_p"))
             .collect()[0])
    if row["n"]:
        lags[batch_id] = (row["n"], row["avg_lag"], row["max_lag"],
                          row["avg_lag_p"], row["max_lag_p"])


q = (trips.writeStream
     .outputMode("append")
     .foreachBatch(measure)
     .option("checkpointLocation", CKPT)
     .trigger(processingTime=f"{TRIGGER} seconds")
     .start())

print()
print(f"  watching {BATCHES} batches at a {TRIGGER} second trigger, topic {TOPIC}", flush=True)
while len([p for p in q.recentProgress if p.get("numInputRows", 0) > 0]) < BATCHES:
    if not q.isActive:
        break
    time.sleep(2)
q.stop()

print()
print(f"  {'batch':>6} {'rows':>7} {'rows/s in':>10} {'rows/s out':>11} "
      f"{'trigger ms':>11} {'avg lag s':>10} {'max lag s':>10} "
      f"{'avg lag s (ms prec)':>20} {'max lag s (ms prec)':>20}   topic {TOPIC}")
csv_rows = []
for p in q.recentProgress:
    n = p.get("numInputRows", 0)
    if not n:
        continue
    bid = p.get("batchId")
    lag = lags.get(bid, (n, None, None, None, None))
    rin = p.get('inputRowsPerSecond') or 0
    rout = p.get('processedRowsPerSecond') or 0
    tms = p.get('durationMs', {}).get('triggerExecution', 0)
    avg_l = lag[1] if lag[1] is not None else 0
    max_l = lag[2] if lag[2] is not None else 0
    avg_p = lag[3] if lag[3] is not None else 0
    max_p = lag[4] if lag[4] is not None else 0
    print(f"  {bid:>6} {n:>7,} {rin:>10.0f} "
          f"{rout:>11.0f} "
          f"{tms:>11,} "
          f"{avg_l:>10} {max_l:>10} "
          f"{avg_p:>20.3f} {max_p:>20.3f}   topic {TOPIC}")
    csv_rows.append([TAG, TOPIC, TRIGGER, bid, n, rin, rout, tms, avg_l, max_l, avg_p, max_p])
print()

PARTS.mkdir(parents=True, exist_ok=True)
with open(PARTS / f"{TAG}.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(["run", "topic", "trigger_s", "batch", "rows", "input_rows_per_s",
                "processed_rows_per_s", "trigger_ms", "avg_lag_s", "max_lag_s",
                "avg_lag_s_precise", "max_lag_s_precise"])
    w.writerows(csv_rows)
print(f"  wrote {PARTS / (TAG + '.csv')}")
print()

spark.stop()
