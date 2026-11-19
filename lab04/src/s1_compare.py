"""Lab 4, Section 9: the two questions, asked of every store that can answer them.

    Q1, the lookup   everything about one zone, asked once per page view
    Q2, the report   the average fare for each pickup hour, over a month of trips

    $SS src/s1_compare.py

Needs lake/zone_summary from Section 5, bda.zone_summary from Section 8, and
bda.trips_jan from the start of Section 9.
"""
import random

from pymongo import MongoClient
from pyspark.sql import functions as F
from l0_common import spark_session, NYC, AGG, LAKE, MONGO, best_of_three, latency_ms

spark = spark_session("lab04-compare")
db = MongoClient(MONGO)["bda"]
db.zone_summary.create_index("zone_id")                     # as Section 8 left it

zone_ids = [r.zone_id for r in spark.read.parquet(f"{AGG}/zone_summary").select("zone_id").collect()]
rnd = random.Random(42)

# --- Q1 ---------------------------------------------------------------------
parquet = spark.read.parquet(f"{AGG}/zone_summary")
delta = spark.read.format("delta").load(f"{LAKE}/zone_summary")

q1 = [
    ("MongoDB, indexed", 1000,
     lambda: db.zone_summary.find_one({"zone_id": rnd.choice(zone_ids)})),
    ("Spark on Parquet", 20,
     lambda: parquet.filter(F.col("zone_id") == rnd.choice(zone_ids)).collect()),
    ("Spark on Delta", 20,
     lambda: delta.filter(F.col("zone_id") == rnd.choice(zone_ids)).collect()),
]

# --- Q2 ---------------------------------------------------------------------
trips = spark.read.parquet(f"{NYC}/curated")                # the same January rows
pipeline = [{"$group": {"_id": "$pickup_hour", "avg_fare": {"$avg": "$fare_amount"}}},
            {"$sort": {"_id": 1}}]
q2 = [
    ("Spark on Parquet",
     lambda: trips.groupBy("pickup_hour").agg(F.avg("fare_amount")).collect()),
    ("MongoDB aggregation",
     lambda: list(db.trips_jan.aggregate(pipeline))),
]

print()
print("  Q1, the lookup: one zone by zone_id, a random zone each time")
print(f"    {'store':22}{'runs':>6}{'median':>12}{'95th pct':>12}")
for name, n, fn in q1:
    med, p95 = latency_ms(fn, n)
    print(f"    {name:22}{n:>6,}{med:>9.1f} ms{p95:>9.1f} ms")
print()
print(f"  Q2, the report: average fare per pickup hour, {trips.count():,} January trips")
print(f"    {'store':22}{'best of 3':>12}")
results = {}
for name, fn in q2:
    results[name] = best_of_three(fn, f"Q2 / {name}")
    print(f"    {name:22}{results[name]:>10.2f} s")
print()

# The two stores must agree, or the timing compares two different answers.
spark_ans = {r[0]: r[1] for r in q2[0][1]()}
mongo_ans = {d["_id"]: d["avg_fare"] for d in q2[1][1]()}
worst = max(abs(spark_ans[h] - mongo_ans[h]) for h in spark_ans)
print(f"  Q2 answers agree across the 24 hours to within {worst:.1e} dollars\n")
spark.stop()
