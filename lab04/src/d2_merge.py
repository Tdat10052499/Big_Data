"""Lab 4, Section 5: apply late corrections to the Delta table with one MERGE.

Lab 3 showed that trips arrive late. Here a batch of corrections arrives after
the summary was built: three zones have new totals, and two zones that had no
pickups at all in Lab 2 have their first ones.

    $SS src/d2_merge.py
"""
from pyspark.sql.types import StructType, IntegerType, LongType, DoubleType
from delta.tables import DeltaTable
from l0_common import spark_session, LAKE

TABLE = f"{LAKE}/zone_summary"
spark = spark_session("lab04-delta-merge")

schema = (StructType()
          .add("zone_id", IntegerType())
          .add("trips", LongType())
          .add("avg_fare", DoubleType())
          .add("avg_distance", DoubleType()))

corrections = spark.createDataFrame([
    (132, 404_850, 63.52, 16.03),     # JFK Airport, 125 late trips
    (161, 418_480, 15.82, 2.36),      # Midtown Center, 78 late trips
    (237, 410_655, 12.59, 1.75),      # Upper East Side South, 45 late trips
    (5,   2,       48.30, 11.90),     # Arden Heights, first pickups ever
    (84,  1,       52.10, 13.40),     # Eltingville, first pickup ever
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
print(f"\n  rows in the table now  {spark.read.format('delta').load(TABLE).count():,}\n")
spark.stop()
