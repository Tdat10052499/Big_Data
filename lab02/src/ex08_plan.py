"""Lab 2, Exercise 8 supplement: the plan and the size of the cache (no timing).

Same session and definitions as src/ex08_cache.py: local[4], f = q1.filter(fare_amount > 10),
the same passes P1..P4.
  (a) P2 on the uncached f: final executed plan (taken after .collect()), its FileScan
      parquet line(s) and ReadSchema.
  (b) f.cache(), P1 fills the cache, then P2 on the cached f: final executed plan, its
      InMemoryTableScan / InMemoryRelation lines and the columns held by the cache.
  (c) the cache size from the Spark UI REST API (storage/rdd), and screenshots of the
      Storage tab (and the RDD details page) taken with headless Chromium while the UI is up.
This run measures no time.
Run: spark-submit ~/bda/lab02/src/ex08_plan.py
"""
import json
import os
import re
import time
import urllib.request
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

HERE = Path(__file__).resolve().parent.parent
MIB = 1024 * 1024

spark = (SparkSession.builder.appName("lab02-ex08-plan").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
port = re.search(r":(\d+)$", sc.uiWebUrl).group(1)
BASE = f"http://localhost:{port}"
API = f"{BASE}/api/v1/applications/{sc.applicationId}"

q1 = spark.read.parquet(CURATED_Q1)
f = q1.filter(F.col("fare_amount") > 10)
f.createOrReplaceTempView("ex08_f")      # lets spark.catalog.isCached see whether f's plan is cached

# the same passes as src/ex08_cache.py, as DataFrames
PASSES = [
    ("P1", "count(*)", lambda d: d.agg(F.count("*").alias("n"))),
    ("P2", "avg fare_amount by PULocationID", lambda d: d.groupBy("PULocationID").agg(F.avg("fare_amount"))),
    ("P3", "sum tip_amount by payment_type", lambda d: d.groupBy("payment_type").agg(F.sum("tip_amount"))),
    ("P4", "max trip_distance by pickup_hour", lambda d: d.groupBy("pickup_hour").agg(F.max("trip_distance"))),
]
P = {name: fn for name, _, fn in PASSES}


def get(path):
    with urllib.request.urlopen(API + path, timeout=10) as r:
        return json.load(r)


def top_level_split(s):
    """Split 'a:int,b:decimal(10,2)' at commas that are not inside () or <>."""
    out, depth, cur = [], 0, ""
    for ch in s:
        if ch in "(<[":
            depth += 1
        elif ch in ")>]":
            depth -= 1
        if ch == "," and depth == 0:
            out.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        out.append(cur.strip())
    return out


def plan_of(df):
    return df._jdf.queryExecution().executedPlan().toString()


print()
print(f"  settings: master {sc.master}, spark.sql.adaptive.enabled={spark.conf.get('spark.sql.adaptive.enabled')}, "
      f"spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')}")
print(f"  spark.driver.memory = {sc.getConf().get('spark.driver.memory', 'not set')}")
for ex in get("/executors"):
    mm = ex.get("maxMemory")
    if mm is not None:
        print(f"  MemoryStore capacity (REST /executors, executor {ex['id']}, maxMemory): {mm:,} bytes = {mm / MIB:.1f} MiB")
print(f"  f = curated_q1.filter(fare_amount > 10); passes: "
      + "; ".join(f"{n} {d}" for n, d, _ in PASSES))
print(f"  cache empty at start: {not spark.catalog.isCached('ex08_f')}")
print()

# ---- (a) uncached P2 -----------------------------------------------------------
u = P["P2"](f)
u_rows = u.collect()
u_plan = plan_of(u)
print(f"  ==== (a) P2 on the uncached f: {len(u_rows)} result rows; final executed plan (after .collect()) ====")
print(u_plan)
print()
print("  FileScan parquet line(s):")
schemas = []
for line in u_plan.splitlines():
    if "FileScan parquet" in line:
        print(f"    {line.strip()}")
        m = re.search(r"ReadSchema: struct<(.*)>\s*$", line)
        if m:
            schemas.append(m.group(1))
for s in dict.fromkeys(schemas):
    cols = top_level_split(s)
    print(f"  ReadSchema: struct<{s}>")
    print(f"  columns in that ReadSchema: {len(cols)} ({', '.join(c.split(':')[0] for c in cols)})"
          + ("   [note: text contains '...', it may be truncated by Spark]" if "..." in s else ""))
print(f"  uncached plan contains FileScan parquet: {'yes' if 'FileScan parquet' in u_plan else 'no'}; "
      f"contains InMemoryTableScan: {'yes' if 'InMemoryTableScan' in u_plan else 'no'}")
print()

# ---- (b) cached P2 -------------------------------------------------------------
f.cache()
n1 = P["P1"](f).collect()[0]["n"]
c = P["P2"](f)
c_rows = c.collect()
c_plan = plan_of(c)
print(f"  ==== (b) f.cache(); P1 filled the cache (count {n1:,}); P2 on the cached f: {len(c_rows)} result rows; "
      f"final executed plan (after .collect()) ====")
print(c_plan)
print()
print("  lines containing InMemoryTableScan or InMemoryRelation:")
rel_lists = []
for line in c_plan.splitlines():
    if "InMemoryTableScan" in line or "InMemoryRelation" in line:
        print(f"    {line.strip()}")
        if "InMemoryRelation" in line:
            m = re.search(r"InMemoryRelation \[([^\]]*)\]", line)
            if m:
                rel_lists.append(m.group(1))
for lst in dict.fromkeys(rel_lists):
    attrs = [a.strip() for a in lst.split(",") if a.strip()]
    print(f"  columns held by the cache (InMemoryRelation, first [...] list): {len(attrs)}")
    print(f"    [{', '.join(attrs)}]")
print(f"  cached plan contains InMemoryTableScan: {'yes' if 'InMemoryTableScan' in c_plan else 'no'}; "
      f"contains FileScan parquet: {'yes' if 'FileScan parquet' in c_plan else 'no'}")
print(f"  results of P2 equal uncached vs cached (sorted, exact): "
      f"{'PASS' if sorted(map(tuple, u_rows)) == sorted(map(tuple, c_rows)) else 'FAIL'}")
print()

# ---- (c) cache size from the REST API ------------------------------------------
rdds = []
for _ in range(30):                     # the UI listener is asynchronous
    rdds = get("/storage/rdd")
    if rdds:
        break
    time.sleep(0.5)
print(f"  ==== (c) storage, from {API}/storage/rdd ====")
if not rdds:
    print("  no cached RDD listed by the REST API")
for r in rdds:
    d = get(f"/storage/rdd/{r['id']}")
    print(f"  RDD id {d['id']}")
    print(f"    name: {d['name']}")
    print(f"    storage level: {d['storageLevel']}")
    print(f"    cached partitions: {d['numCachedPartitions']} / {d['numPartitions']}")
    print(f"    memoryUsed: {d['memoryUsed']:,} bytes = {d['memoryUsed'] / MIB:.1f} MiB")
    print(f"    diskUsed:   {d['diskUsed']:,} bytes = {d['diskUsed'] / MIB:.1f} MiB")
    print(f"    fully cached (numCachedPartitions == numPartitions): "
          f"{'PASS' if d['numCachedPartitions'] == d['numPartitions'] else 'FAIL (partially cached)'}")
    for dd in d.get("dataDistribution", []) or []:
        print(f"    data distribution {dd.get('address')}: memoryUsed {dd.get('memoryUsed', 0):,} bytes, "
              f"memoryRemaining {dd.get('memoryRemaining', 0):,} bytes, diskUsed {dd.get('diskUsed', 0):,} bytes")
print()

# ---- screenshots of the Storage tab while the UI is up ---------------------------
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    exe = os.environ.get("CHROMIUM_PATH")
    b = p.chromium.launch(executable_path=exe) if exe else p.chromium.launch()
    pg = b.new_page(viewport={"width": 1400, "height": 900})
    pg.goto(f"{BASE}/storage/", wait_until="networkidle")
    pg.screenshot(path=str(HERE / "figures" / "lab02_ex08_storage.png"), full_page=True)
    print("  screenshot: figures/lab02_ex08_storage.png (Storage tab)")
    if rdds:
        pg.goto(f"{BASE}/storage/rdd/?id={rdds[0]['id']}", wait_until="networkidle")
        pg.screenshot(path=str(HERE / "figures" / "lab02_ex08_storage_detail.png"), full_page=True)
        print(f"  screenshot: figures/lab02_ex08_storage_detail.png (RDD {rdds[0]['id']} details page)")
    b.close()
print()

f.unpersist(blocking=True)
print(f"  cache empty at end: {not spark.catalog.isCached('ex08_f')}")
print("  This run measures no time; it records plans and cache size only.")
print()
spark.stop()
