"""Lab 2, Exercise 4: run a few actions, then keep the Spark UI open for screenshots.

Actions (each one is a Spark job or more):
  1. curated_q1 count
  2. wide aggregation: groupBy(PULocationID, DOLocationID, pickup_hour).count(), then count the groups
  3. join to the zone lookup on PULocationID, trips per Borough
Adaptive Query Execution is switched OFF here, so each action is one Spark job and
each stage boundary in a job is one shuffle (with AQE on, every shuffle stage is
submitted as a separate job, which hides the stage count the exercise asks for).
After the actions the program writes runs/ex04_ui.ready (the UI address) and keeps
running, so the UI on port 4040 stays up, until runs/ex04_ui.stop exists (max 20 min).
Run: spark-submit ~/bda/lab02/src/ex04_job.py
"""
import time
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1, REF_ZONES

RUNS = Path(__file__).resolve().parent.parent / "runs"
READY, STOP = RUNS / "ex04_ui.ready", RUNS / "ex04_ui.stop"
for f in (READY, STOP):
    f.unlink(missing_ok=True)

spark = (SparkSession.builder.appName("lab02-ex04-stages").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
spark.conf.set("spark.sql.adaptive.enabled", "false")

q1 = spark.read.parquet(CURATED_Q1)
zones = (spark.read.option("header", True).csv(REF_ZONES)
         .withColumn("LocationID", F.col("LocationID").cast("int")))

print()
print(f"  settings: master local[4], spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  application id {sc.applicationId}, UI {sc.uiWebUrl}")
print()

sc.setJobGroup("ex04-1", "action 1: count curated_q1")
print(f"  action 1  count curated_q1                     {q1.count():,} rows")

sc.setJobGroup("ex04-2", "action 2: wide aggregation, count groups")
groups = q1.groupBy("PULocationID", "DOLocationID", "pickup_hour").count().count()
print(f"  action 2  wide aggregation groups               {groups:,}")

sc.setJobGroup("ex04-3", "action 3: join zones, trips per borough")
boroughs = (q1.join(zones, q1.PULocationID == zones.LocationID, "left")
              .groupBy("Borough").agg(F.count("*").alias("trips")).collect())
print(f"  action 3  join + groupBy(Borough) rows          {len(boroughs)}")
print()

READY.write_text(sc.uiWebUrl + "\n")
print(f"  UI is up; waiting for {STOP.name} (max 20 min)", flush=True)
deadline = time.time() + 20 * 60
while not STOP.exists() and time.time() < deadline:
    time.sleep(1)
STOP.unlink(missing_ok=True)
READY.unlink(missing_ok=True)
spark.stop()
