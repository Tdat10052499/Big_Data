"""Lab 4, Section 5: turn the Lab 2 zone summary into a Delta table.

Starts from scratch every time it runs: it deletes lake/zone_summary first,
so the table it creates is always version 0.

    $SS src/d1_create.py
"""
import subprocess

from delta.tables import DeltaTable
from l0_common import spark_session, AGG, LAKE

TABLE = f"{LAKE}/zone_summary"
subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-rm", "-r", "-f", TABLE],
               capture_output=True)

spark = spark_session("lab04-delta-create")

summary = spark.read.parquet(f"{AGG}/zone_summary")        # Lab 2, 258 rows
summary.write.format("delta").save(TABLE)

table = DeltaTable.forPath(spark, TABLE)
last = table.history(1).first()

print()
print(f"  table      {TABLE}")
print(f"  rows       {spark.read.format('delta').load(TABLE).count():,}")
print(f"  version    {last['version']}, written by the operation {last['operation']}")
print()
spark.stop()
