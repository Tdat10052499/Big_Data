"""Lab 4, Exercise 8: append instead of overwrite (copied from d4_readers.py).

One thread plays a nightly job. It starts from the full January table, then APPENDS
the short trips (trip_distance <= 2) six times with mode("append"). A second thread
plays a dashboard and counts the rows, again and again, while the job runs.

A whole table now has 2,724,143 + k x 1,596,003 rows for k = 0..6; any other count,
or an error, is a reader that saw something else.

    $SS src/e8_append.py parquet          # each append as Spark writes it
    $SS src/e8_append.py delta
    $SS src/e8_append.py parquet 64       # each append written as 64 files
    $SS src/e8_append.py delta 64
"""
import re
import sys
import threading
import time
from collections import Counter

from pyspark.sql import functions as F
from l0_common import spark_session, NYC, LAKE

FMT = sys.argv[1] if len(sys.argv) > 1 else "parquet"
FILES = int(sys.argv[2]) if len(sys.argv) > 2 else None   # None: as Spark writes it
REWRITES = 6
TARGET = f"{LAKE}/append_{FMT}"

spark = spark_session(f"lab04-append-{FMT}")
jan = spark.read.parquet(f"{NYC}/curated")                 # 2,724,143 rows
short = jan.filter(F.col("trip_distance") <= 2)            # the shorter trips only
n_jan, n_short = jan.count(), short.count()
sizes = [n_jan + k * n_short for k in range(REWRITES + 1)]
to_append = short.repartition(FILES) if FILES else short

# Start from a complete table, written before the reader begins.
jan.write.format(FMT).mode("overwrite").save(TARGET)

done = threading.Event()
seen = []                                                   # one entry per read


def nightly_job():
    for i in range(REWRITES):
        to_append.write.format(FMT).mode("append").save(TARGET)
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
print(f"  files per append                {FILES if FILES else 'as Spark writes it'}")
print(f"  short trips appended            {REWRITES} times in {elapsed:.0f} s")
print(f"  the whole sizes                 {n_jan:,} + k x {n_short:,} rows, k = 0..{REWRITES}")
for k in range(REWRITES + 1):
    print(f"      k = {k}   {sizes[k]:>12,} rows   seen {tally.get(sizes[k], 0):>4,} times")
print(f"  reads made while it ran         {len(seen):,}")
print(f"  reads that saw a whole table    {complete:,}")
print(f"  reads that saw anything else    {len(seen) - complete:,}")
for k, n in sorted(others.items(), key=lambda kv: -kv[1]):
    label = f"{k:,} rows" if isinstance(k, int) else k
    print(f"      {label:<26}{n:>6,} {'time' if n == 1 else 'times'}")
print()
spark.stop()
