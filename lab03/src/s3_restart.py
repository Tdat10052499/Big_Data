"""Lab 3, Sections 9 and 10: checkpoints, replay, and surviving a crash.

Appends every event it reads to HDFS, with a checkpoint. If the program is killed
part way through, running it again carries on from the last committed batch.

    python src/trip_producer.py --topic taxi-restart --limit 40000 --rate 6000
    spark-submit ... src/s3_restart.py          # kill this one part way through
    spark-submit ... src/s3_restart.py          # then run it again
    spark-submit ... src/s3_restart.py --check  # count what landed

Because every trip_id is unique, counting rows and counting distinct trip_ids
answers both questions at once. Equal numbers mean nothing was written twice.
Matching the number produced means nothing was lost.

Add --reset to start again from an empty topic.
"""
import subprocess
import sys
import time

from pyspark.sql import functions as F
from s0_common import spark_session, SCHEMA, BOOTSTRAP

TOPIC = "taxi-restart"
STREAM = "hdfs://localhost:9000/user/tdat1/nyc/stream"
CKPT = f"{STREAM}/ck/restart"
OUT = f"{STREAM}/restart"
KT = "/opt/kafka/bin/kafka-topics.sh"
HDFS = "/opt/hadoop/bin/hdfs"


def topic_exists():
    r = subprocess.run([KT, "--bootstrap-server", BOOTSTRAP, "--list"],
                       capture_output=True, text=True)
    return TOPIC in r.stdout.split()


if "--reset" in sys.argv:
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
    subprocess.run([HDFS, "dfs", "-rm", "-r", "-f", CKPT, OUT], capture_output=True)
    print(f"  topic {TOPIC} recreated, checkpoint and output cleared")
    sys.exit(0)

spark = spark_session("lab03-restart")

if "--check" in sys.argv:
    try:
        df = spark.read.parquet(OUT)
        rows = df.count()
        distinct = df.select("trip_id").distinct().count()
    except Exception:
        rows = distinct = 0
    print()
    print(f"  rows written to HDFS from topic {TOPIC}      {rows:>8,}")
    print(f"  distinct trip_id from topic {TOPIC}          {distinct:>8,}")
    print(f"  duplicates from topic {TOPIC}                {rows - distinct:>8,}")
    print()
    spark.stop()
    sys.exit(0)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 2000)
       .load())

trips = (raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("t"))
            .select("t.*")
            .withColumn("event_time", F.to_timestamp("event_time")))

q = (trips.writeStream
     .outputMode("append")
     .format("parquet")
     .option("path", OUT)
     .option("checkpointLocation", CKPT)
     .trigger(processingTime="2 seconds")
     .start())
from datetime import datetime, timezone  # tdat1 edit
print(f"  query started at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]} UTC", flush=True)

print("  query running. Batches of 2,000 events every 2 seconds.")
print("  Kill this program with Ctrl+C, or from another terminal, then run it again.")

try:
    q.awaitTermination()
except KeyboardInterrupt:
    print("\n  interrupted")

spark.stop()
