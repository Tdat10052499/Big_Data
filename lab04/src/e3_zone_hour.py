"""Lab 4, Exercise 3: a Delta table of my own, built from Lab 2's agg/zone_hour.

Copied from d1_create.py and changed: it builds lake/zone_hour from agg/zone_hour
(version 0), removes every row for pickup hour 3 with one DELETE (version 1), then
prints the history, the row count at each version and the operationMetrics of the delete.

    $SS src/e3_zone_hour.py
"""
import subprocess

from delta.tables import DeltaTable
from l0_common import spark_session, AGG, LAKE

TABLE = f"{LAKE}/zone_hour"
subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", TABLE],
               capture_output=True)

spark = spark_session("lab04-e3-zone-hour")

zone_hour = spark.read.parquet(f"{AGG}/zone_hour")           # Lab 2
zone_hour.write.format("delta").save(TABLE)
print()
print(f"  table      {TABLE}")
print(f"  version 0  {spark.read.format('delta').load(TABLE).count():,} rows, "
      f"{spark.read.format('delta').load(TABLE).filter('pickup_hour = 3').count():,} of them for pickup hour 3")

table = DeltaTable.forPath(spark, TABLE)
table.delete("pickup_hour = 3")

print()
print("  the history, newest first")
spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`") \
     .select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

print("  row count at each version")
latest = table.history(1).first()["version"]
for v in range(latest + 1):
    df = spark.read.format("delta").option("versionAsOf", v).load(TABLE)
    print(f"    version {v}   {df.count():>6,} rows   hour 3 rows {df.filter('pickup_hour = 3').count():>4,}")
print()

last = table.history(1).first()
print(f"  operationMetrics of version {last['version']} ({last['operation']})")
for k, v in sorted(last["operationMetrics"].items()):
    print(f"    {k:34}{v:>12}")
print()
spark.stop()
