"""Lab 4, Section 5: the table's history, its past versions, and the two rules it enforces.

    $SS src/d3_history.py
"""
import re
import textwrap

from pyspark.sql import functions as F
from l0_common import spark_session, LAKE

TABLE = f"{LAKE}/zone_summary"
spark = spark_session("lab04-delta-history")


def refusal(e):
    """The first sentence of an error, wrapped to fit the terminal."""
    first = " ".join(str(e).split())
    first = re.split(r"(?<=[.?])\s", first, maxsplit=1)[0]
    return textwrap.fill(first, width=74, initial_indent="     refused: ",
                         subsequent_indent=" " * 14)

print()
print("  1. the history, newest first")
spark.sql(f"DESCRIBE HISTORY delta.`{TABLE}`") \
     .select("version", "timestamp", "operation", "userName").show(truncate=False)

print("  2. the same path read at two versions")
for label, reader in (("version 0", spark.read.format("delta").option("versionAsOf", 0)),
                      ("latest   ", spark.read.format("delta"))):
    df = reader.load(TABLE)
    jfk = df.filter("zone_id = 132").first()["trips"]
    print(f"     {label}   {df.count():>4,} rows   zone 132 has {jfk:,} trips")
print()

print("  3. an append that brings a column the table does not have")
extra = spark.read.format("delta").load(TABLE).limit(1).withColumn("surge", F.lit(1.5))
try:
    extra.write.format("delta").mode("append").save(TABLE)
    print("     accepted")
except Exception as e:
    print(refusal(e))
print()

print("  4. removing old files that no version needs any more")
dry = spark.sql(f"VACUUM delta.`{TABLE}` RETAIN 168 HOURS DRY RUN")
print(f"     keep 7 days of history: {dry.count()} file(s) would be deleted")
print("     keep no history:")
try:
    spark.sql(f"VACUUM delta.`{TABLE}` RETAIN 0 HOURS DRY RUN").count()
    print("     accepted")
except Exception as e:
    print(refusal(e))
print()
spark.stop()
