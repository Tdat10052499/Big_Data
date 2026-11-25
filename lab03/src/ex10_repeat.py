"""Lab 3, Exercise 10 evidence: does an interrupted batch run again?

Reads taxi-restart from earliest (2,000 offsets per batch, 2 s trigger) with its own checkpoint.
For every foreachBatch call it first appends one line to runs/ex10_side_effects.txt
(flush + fsync). That line stands for an e-mail or a payment: once sent, it cannot be recalled.
Then it sleeps 3 s on purpose, to widen the window so that a kill lands inside a batch.
    $SS src/ex10_repeat.py --reset    clear the checkpoint and empty the side-effects file
    $SS src/ex10_repeat.py            run until killed
"""
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from s0_common import spark_session, BOOTSTRAP

TOPIC = "taxi-restart"
CKPT = "hdfs://localhost:9000/user/tdat1/nyc/stream/ck/ex10_repeat"
SIDE = Path(__file__).resolve().parent.parent / "runs" / "ex10_side_effects.txt"


def utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


print(f"  topic {TOPIC}")
print(f"  checkpoint {CKPT}")
print(f"  side-effects file {SIDE}")
print("  each batch: append one line to the side-effects file (flush + fsync), then time.sleep(3);")
print("  the sleep is a deliberate widening of the window so that the kill lands inside a batch", flush=True)

if "--reset" in sys.argv:
    subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", CKPT], capture_output=True)
    SIDE.write_text("")
    print(f"  checkpoint cleared, side-effects file emptied")
    sys.exit(0)

spark = spark_session("lab03-ex10-repeat")
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 2000)
       .load())


def side_effect(df, batch_id):
    n = df.count()
    with open(SIDE, "a") as fh:
        fh.write(f"{utc()} batch_id={batch_id} rows={n} pid={os.getpid()}\n")
        fh.flush()
        os.fsync(fh.fileno())
    print(f"  {utc()} UTC batch_id={batch_id}: {n:,} rows from topic {TOPIC}, side effect written, sleeping 3 s", flush=True)
    time.sleep(3)


q = (raw.writeStream
     .foreachBatch(side_effect)
     .option("checkpointLocation", CKPT)
     .trigger(processingTime="2 seconds")
     .start())
print(f"  query started at {utc()} UTC", flush=True)
try:
    q.awaitTermination()
except KeyboardInterrupt:
    print("\n  interrupted")
spark.stop()
