"""Lab 4, Exercise 6: a MERGE predicted before it runs (copied from d2_merge.py).

Seven corrections: four for zones the table already has, three for zones that
had no pickups in Lab 2 (103, 104, 109). Run d1_create.py first so the MERGE
starts from version 0. It also prints the number of data files before and after.

    $SS src/d1_create.py     # back to version 0 first
    $SS src/e6_merge.py
"""
from pyspark.sql.types import StructType, IntegerType, LongType, DoubleType
from delta.tables import DeltaTable
from l0_common import spark_session, LAKE

TABLE = f"{LAKE}/zone_summary"
spark = spark_session("lab04-e6-merge")

schema = (StructType()
          .add("zone_id", IntegerType())
          .add("trips", LongType())
          .add("avg_fare", DoubleType())
          .add("avg_distance", DoubleType()))

# Exercise 6: four zones that exist in the table and three that do not
# (103, 104, 109 had no pickups in Lab 2's data, manual Exercise 6).
# The figures are hypothetical correction values chosen for the exercise, not measurements;
# only the zone_ids matter for the MERGE metrics.
corrections = spark.createDataFrame([
    (132, 404_900, 63.50, 16.02),     # exists
    (161, 418_500, 15.82, 2.36),      # exists
    (237, 410_700, 12.59, 1.75),      # exists
    (236, 400_000, 13.00, 1.90),      # exists
    (103, 1,       20.00, 3.00),      # no pickups in Lab 2
    (104, 1,       20.00, 3.00),      # no pickups in Lab 2
    (109, 1,       20.00, 3.00),      # no pickups in Lab 2
], schema)

print()
print("  the corrections")
corrections.show()

table = DeltaTable.forPath(spark, TABLE)
(table.alias("t")
      .merge(corrections.alias("c"), "t.zone_id = c.zone_id")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute())

last = table.history(1).first()
m = last["operationMetrics"]
print(f"  version {last['version']}, operation {last['operation']}")
for key in ("numSourceRows", "numTargetRowsUpdated", "numTargetRowsInserted",
            "numTargetRowsCopied", "numTargetFilesRemoved", "numTargetFilesAdded"):
    print(f"    {key:26}{int(m[key]):>6,}")
print(f"  data files in the table before the MERGE  {len(spark.read.format('delta').option('versionAsOf', last['version'] - 1).load(TABLE).inputFiles())}")
print(f"  data files in the table after the MERGE   {len(spark.read.format('delta').load(TABLE).inputFiles())}")
print(f"\n  rows in the table now  {spark.read.format('delta').load(TABLE).count():,}\n")
spark.stop()
