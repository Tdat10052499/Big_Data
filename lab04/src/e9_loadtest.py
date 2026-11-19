"""Lab 4, Exercise 9: how many Q1 lookups a second, from several threads at once.

Q1 = everything about one zone, by zone_id, a random zone each request (seed 42 per thread).
MongoDB (bda.zone_summary, indexed on zone_id) from 1, 4 and 16 threads; Spark on Parquet
(agg/zone_summary) from 1 and 4 threads. Each level: a 2 s warm-up that is not scored,
then 10 s measured. Reports requests per second, median and 95th percentile latency,
and writes out/loadtest.csv with one row per store and thread count.

    $SS src/e9_loadtest.py
"""
import csv
import os
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from pymongo import MongoClient
from pyspark.sql import functions as F
from l0_common import spark_session, AGG, MONGO

DURATION, WARMUP = 10.0, 2.0
spark = spark_session("lab04-e9-loadtest")
client = MongoClient(MONGO, maxPoolSize=64)           # one client, shared by all threads
coll = client["bda"]["zone_summary"]
coll.create_index("zone_id")
parquet = spark.read.parquet(f"{AGG}/zone_summary")
zone_ids = [r.zone_id for r in parquet.select("zone_id").collect()]


def mongo_q1(z):
    return coll.find_one({"zone_id": z})


def spark_q1(z):
    return parquet.filter(F.col("zone_id") == z).collect()


def worker(fn, seconds, seed, out):
    rnd = random.Random(seed)
    end = time.perf_counter() + seconds
    while time.perf_counter() < end:
        z = rnd.choice(zone_ids)
        t0 = time.perf_counter()
        fn(z)
        out.append((time.perf_counter() - t0) * 1000)


def level(fn, threads):
    with ThreadPoolExecutor(threads) as ex:                   # warm-up, not scored
        list(ex.map(lambda i: worker(fn, WARMUP, 1000 + i, []), range(threads)))
    lat = []
    lock = threading.Lock()

    def run(i):
        mine = []
        worker(fn, DURATION, 42 + i, mine)
        with lock:
            lat.extend(mine)

    t0 = time.perf_counter()
    with ThreadPoolExecutor(threads) as ex:
        list(ex.map(run, range(threads)))
    wall = time.perf_counter() - t0
    lat.sort()
    p95 = lat[max(0, int(round(0.95 * len(lat))) - 1)]
    return len(lat), wall, len(lat) / wall, statistics.median(lat), p95


plan = [("MongoDB, indexed", mongo_q1, 1), ("MongoDB, indexed", mongo_q1, 4),
        ("MongoDB, indexed", mongo_q1, 16),
        ("Spark on Parquet", spark_q1, 1), ("Spark on Parquet", spark_q1, 4)]

print()
print(f"  Q1 load test: {len(zone_ids)} zones, {DURATION:.0f} s per level after a {WARMUP:.0f} s warm-up, "
      f"cores {os.cpu_count()}, Spark local[4]")
print(f"  {'store':20}{'threads':>8}{'requests':>10}{'seconds':>9}{'req/s':>10}{'median ms':>11}{'p95 ms':>9}")
rows = []
for store, fn, n in plan:
    req, wall, rps, med, p95 = level(fn, n)
    rows.append({"store": store, "threads": n, "requests": req, "seconds": round(wall, 2),
                 "requests_per_s": round(rps, 1), "median_ms": round(med, 3), "p95_ms": round(p95, 3)})
    print(f"  {store:20}{n:>8}{req:>10,}{wall:>9.2f}{rps:>10,.1f}{med:>11.3f}{p95:>9.3f}")

os.makedirs("out", exist_ok=True)
with open("out/loadtest.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)

print()
for store in ("MongoDB, indexed", "Spark on Parquet"):
    ok = [r for r in rows if r["store"] == store and r["p95_ms"] < 10]
    if ok:
        best = max(ok, key=lambda r: r["requests_per_s"])
        print(f"  {store}: highest measured rate with p95 < 10 ms = {best['requests_per_s']:,} req/s "
              f"({best['threads']} threads, p95 {best['p95_ms']} ms)")
    else:
        print(f"  {store}: p95 never stayed under 10 ms at the levels measured")
print("  wrote out/loadtest.csv\n")
spark.stop()
