"""Lab 4, Section 4: the same rows stored two ways, a row at a time and a column at a time.

    $SS src/f1_formats.py --write        # write January as CSV and as Parquet, once
    $SS src/f1_formats.py                # measure what three queries read from each

Exercise 1 repeats it for another month of curated_q1:
    $SS src/f1_formats.py --write --month 2024-02
    $SS src/f1_formats.py --month 2024-02
"""
import sys

from pyspark.sql import functions as F
from l0_common import (spark_session, NYC, FMT, hdfs_bytes, bytes_read,
                       best_of_three, mb)

month = sys.argv[sys.argv.index("--month") + 1] if "--month" in sys.argv else "2024-01"
CSV = f"{FMT}/{month}/csv"
PARQUET = f"{FMT}/{month}/parquet"

spark = spark_session("lab04-formats")

if month == "2024-01":
    source = spark.read.parquet(f"{NYC}/curated")          # Lab 1, January only
else:
    source = (spark.read.parquet(f"{NYC}/curated_q1")      # Lab 2, three months
              .filter(F.date_format("pickup_date", "yyyy-MM") == month))

if "--write" in sys.argv:
    source.write.mode("overwrite").option("header", True).csv(CSV)
    source.write.mode("overwrite").parquet(PARQUET)
    print(f"\n  wrote {source.count():,} rows of {month} twice:")
    print(f"    {CSV}")
    print(f"    {PARQUET}\n")
    spark.stop()
    sys.exit(0)

# A CSV file carries no types, so give the reader the schema the Parquet file
# already holds. Asking Spark to infer it instead would read every byte once
# more before the query even starts.
pq = spark.read.parquet(PARQUET)
csv = spark.read.option("header", True).schema(pq.schema).csv(CSV)

queries = [
    ("count the rows",      lambda df: df.count()),
    ("sum one column",      lambda df: df.agg(F.sum("fare_amount")).collect()),
    ("count every column",  lambda df: df.agg(*[F.count(c) for c in df.columns]).collect()),
]

csv_bytes, pq_bytes = hdfs_bytes(CSV), hdfs_bytes(PARQUET)
rows = pq.count()

print()
print(f"  {month}, {rows:,} rows, {len(pq.columns)} columns, the same rows in both formats")
print()
print(f"  {'':22}{'CSV':>12}{'Parquet':>12}{'ratio':>9}")
print(f"  {'stored on HDFS':22}{mb(csv_bytes):>12}{mb(pq_bytes):>12}{csv_bytes / pq_bytes:>8.1f}x")
print()
print(f"  {'':22}{'bytes read':^24}  {'time, best of 3':^20}")
print(f"  {'query':22}{'CSV':>12}{'Parquet':>12}  {'CSV':>9}{'Parquet':>11}")
for name, q in queries:
    _, b_csv = bytes_read(spark, lambda: q(csv))
    _, b_pq = bytes_read(spark, lambda: q(pq))
    t_csv = best_of_three(lambda: q(csv), f"{name} / CSV")
    t_pq = best_of_three(lambda: q(pq), f"{name} / Parquet")
    print(f"  {name:22}{mb(b_csv):>12}{mb(b_pq):>12}  {t_csv:>7.2f} s{t_pq:>9.2f} s")
print()

if "--plan" in sys.argv:
    for label, df in (("CSV", csv), ("Parquet", pq)):
        plan = spark._jvm.PythonSQLUtils.explainString(
            df.agg(F.sum("fare_amount"))._jdf.queryExecution(), "simple")
        scan = [l.strip() for l in plan.splitlines() if "FileScan" in l][0]
        print(f"  {label} scan for 'sum one column':")
        print(f"    {scan[:scan.index('[')]}... {scan[scan.index('ReadSchema'):]}")
    print()

spark.stop()
