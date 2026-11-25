"""Lab 3, Sections 7 and 8: windows, and what a watermark throws away.

This script runs the windowed count ONCE and reports two numbers: how many rows
it read from Kafka, and how many trips actually reached a window. You run it
twice, with a producer call in between, and compare.

    # phase 1, on time events
    python src/trip_producer.py --topic taxi-late-demo --limit 10000 \
        --late-fraction 0 --rate 4000
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
        --py-files src/s0_common.py src/s2_watermark.py

    # phase 2, events stamped half an hour in the past
    python src/trip_producer.py --topic taxi-late-demo --limit 2000 \
        --late-fraction 1.0 --late-seconds 1800 --rate 4000
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 \
        --py-files src/s0_common.py src/s2_watermark.py

The second run keeps the same checkpoint, so it reads only the new events. Its
watermark is already far past them, and none of them is counted.

Add --reset to start the whole exercise again from an empty topic.
"""
import os
import subprocess
import sys
import time

from pyspark.sql import functions as F
from s0_common import spark_session, SCHEMA, BOOTSTRAP

TOPIC = "taxi-late-demo"
STREAM = "hdfs://localhost:9000/user/thaianh/nyc/stream"
# Spell the checkpoint out in full. A bare path would land in HDFS anyway,
# because fs.defaultFS is HDFS, and then a local delete would not touch it.
CKPT = f"{STREAM}/ck/late_demo"
OUT = f"{STREAM}/late_demo"
KT = "/opt/kafka/bin/kafka-topics.sh"


def topic_exists():
    r = subprocess.run([KT, "--bootstrap-server", BOOTSTRAP, "--list"],
                       capture_output=True, text=True)
    return TOPIC in r.stdout.split()


def reset():
    """Delete and recreate the topic, clear the checkpoint and the output."""
    if topic_exists():
        subprocess.run([KT, "--bootstrap-server", BOOTSTRAP, "--delete", "--topic", TOPIC],
                       capture_output=True, text=True)
        for _ in range(30):
            if not topic_exists():
                break
            time.sleep(1)
    subprocess.run([KT, "--bootstrap-server", BOOTSTRAP, "--create", "--topic", TOPIC,
                    "--partitions", "3", "--replication-factor", "1"],
                   capture_output=True, text=True)
    for _ in range(30):
        if topic_exists():
            break
        time.sleep(1)
    subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", CKPT, OUT],
                   capture_output=True)
    print(f"  topic {TOPIC} recreated, checkpoint and output cleared")


if "--reset" in sys.argv:
    reset()
    sys.exit(0)

spark = spark_session("lab03-watermark")

# Name this run after how many batches the checkpoint has already committed,
# so the two runs can be told apart in the output.
_c = subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-ls", f"{CKPT}/commits"],
                    capture_output=True, text=True).stdout
phase = "run%d" % sum(1 for l in _c.splitlines() if l.startswith("-"))

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("failOnDataLoss", "false")
       .load())

trips = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"))
            .select("t.*")
            .withColumn("event_time", F.to_timestamp("event_time")))

counted = (trips
           .withWatermark("event_time", "2 minutes")
           .groupBy(F.window("event_time", "1 minute"))
           .agg(F.count("*").alias("trips")))


# The memory sink cannot resume from a checkpoint, and resuming is the whole
# point of the second run, so the window counts go to HDFS through foreachBatch.
def write_batch(df, batch_id):
    (df.withColumn("phase", F.lit(phase))
       .write.mode("append").parquet(OUT))


q = (counted.writeStream
     .outputMode("update")
     .foreachBatch(write_batch)
     .option("checkpointLocation", CKPT)
     .trigger(availableNow=True).start())
q.awaitTermination()

read_rows = sum(p.get("numInputRows", 0) for p in q.recentProgress)
try:
    this_run = (spark.read.parquet(OUT).filter(F.col("phase") == phase)
                .agg(F.sum("trips")).collect()[0][0]) or 0
    all_runs = spark.read.parquet(OUT).agg(F.sum("trips")).collect()[0][0] or 0
except Exception:
    this_run = all_runs = 0

print()
print(f"  rows read from Kafka this run     {read_rows:>8,}")
print(f"  trips counted into windows        {this_run:>8,}")
if read_rows:
    print(f"  read but never counted            {read_rows - this_run:>8,}"
          f"   {(read_rows - this_run)/read_rows:.0%} of the {read_rows:,} read")
print(f"  trips counted across all runs     {all_runs:>8,}")
print()

spark.stop()
