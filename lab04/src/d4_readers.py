"""Lab 4, Section 6: what a reader sees while somebody else rewrites the table.

One thread plays a nightly job. It overwrites a table six times, swapping
between two versions of different size. A second thread plays a dashboard.
It counts the rows in the table, again and again, for as long as the job runs.

Every count ought to be one of the two sizes. Any other answer is a reader that
saw the table half written. An error is a reader that saw it half deleted.

    $SS src/d4_readers.py parquet
    $SS src/d4_readers.py delta
"""
import re
import sys
import threading
import time
from collections import Counter

from pyspark.sql import functions as F
from l0_common import spark_session, NYC, LAKE

FMT = sys.argv[1] if len(sys.argv) > 1 else "parquet"
REWRITES = 6
TARGET = f"{LAKE}/readers_{FMT}"

spark = spark_session(f"lab04-readers-{FMT}")
jan = spark.read.parquet(f"{NYC}/curated")                 # 2,724,143 rows
short = jan.filter(F.col("trip_distance") <= 2)            # the shorter trips only
versions = [jan, short]
sizes = [v.count() for v in versions]

# Start from a complete table, written before the reader begins.
jan.write.format(FMT).mode("overwrite").save(TARGET)

done = threading.Event()
seen = []                                                   # one entry per read


def nightly_job():
    for i in range(REWRITES):
        versions[(i + 1) % 2].write.format(FMT).mode("overwrite").save(TARGET)
    done.set()


def read_once():
    if FMT == "parquet":
        # A dashboard knows the columns it expects, so it passes the schema.
        df = spark.read.schema(jan.schema).parquet(TARGET)
    else:
        df = spark.read.format("delta").load(TARGET)
    return df.select(F.count(F.lit(1))).first()[0]


def error_name(e):
    text = str(e)
    for marker in ("FileNotFoundException", "PATH_NOT_FOUND", "UNABLE_TO_INFER_SCHEMA"):
        if marker in text:
            return marker
    m = re.search(r"\[([A-Z_]{6,})\]", text)
    return m.group(1) if m else type(e).__name__


def dashboard():
    while not done.is_set():
        try:
            seen.append(read_once())
        except Exception as e:
            seen.append(error_name(e))


writer = threading.Thread(target=nightly_job)
reader = threading.Thread(target=dashboard)
t0 = time.time()
writer.start()
reader.start()
writer.join()
reader.join()
elapsed = time.time() - t0

tally = Counter(seen)
complete = sum(n for k, n in tally.items() if k in sizes)
others = {k: n for k, n in tally.items() if k not in sizes}

print()
print(f"  format                          {FMT}")
print(f"  table rewritten                 {REWRITES} times in {elapsed:.0f} s")
print(f"  the two whole sizes             {sizes[0]:,} and {sizes[1]:,} rows")
print(f"  reads made while it ran         {len(seen):,}")
print(f"  reads that saw a whole table    {complete:,}")
print(f"  reads that saw anything else    {len(seen) - complete:,}")
for k, n in sorted(others.items(), key=lambda kv: -kv[1]):
    label = f"{k:,} rows" if isinstance(k, int) else k
    print(f"      {label:<26}{n:>6,} {'time' if n == 1 else 'times'}")
print()
spark.stop()
