"""Lab 2, Exercise 6: Adaptive Query Execution on versus off for the Exercise 3 aggregation
    q1.groupBy("payment_type").agg(F.avg("tip_amount"))  (collected)

spark.sql.shuffle.partitions is left at its default. Setting A = AQE on, B = AQE off,
set with spark.conf.set before each DataFrame is built.
Protocol: one warm-up per setting (not scored), then three scored rounds in the
order A,B | B,A | A,B. After the last scored run of each setting the plan of that
executed DataFrame is written to runs/ex06_plan_aqe_on.log / runs/ex06_plan_aqe_off.log,
and the post-shuffle stage's task count is read from the Spark UI REST API.
Refuses to run unless runs/ex06_prediction.txt exists.
Run: spark-submit ~/bda/lab02/src/ex06_aqe.py
"""
import csv
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from pyspark.sql import SparkSession, functions as F
from common import CURATED_Q1

HERE = Path(__file__).resolve().parent.parent
PRED = HERE / "runs" / "ex06_prediction.txt"
if not PRED.exists() or not PRED.read_text().strip():
    sys.exit("STOP: runs/ex06_prediction.txt is missing or empty; the prediction file must exist first.")

spark = (SparkSession.builder.appName("lab02-ex06-aqe").master("local[4]").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
port = re.search(r":(\d+)$", sc.uiWebUrl).group(1)
API = f"http://localhost:{port}/api/v1/applications/{sc.applicationId}"

q1 = spark.read.parquet(CURATED_Q1)
SETTINGS = [("A", "true"), ("B", "false")]
LABEL = {"A": "AQE on  (spark.sql.adaptive.enabled=true)", "B": "AQE off (spark.sql.adaptive.enabled=false)"}


def run(setting, group=None):
    spark.conf.set("spark.sql.adaptive.enabled", dict(SETTINGS)[setting])
    df = q1.groupBy("payment_type").agg(F.avg("tip_amount").alias("avg_tip"))
    if group:
        sc.setJobGroup(group, f"ex06 last scored run, setting {setting}")
    t0 = time.perf_counter()
    rows = df.collect()
    dt = time.perf_counter() - t0
    if group:
        sc.setLocalProperty("spark.jobGroup.id", None)
    return dt, rows, df


def get(path):
    with urllib.request.urlopen(API + path, timeout=10) as r:
        return json.load(r)


def post_shuffle_stages(group):
    """Stages that ran (status COMPLETE) in the jobs of this job group and read shuffle data."""
    for _ in range(30):                       # the UI listener is asynchronous: wait until it caught up
        jobs = [j for j in get("/jobs") if j.get("jobGroup") == group]
        if jobs and all(j["status"] == "SUCCEEDED" for j in jobs):
            break
        time.sleep(0.5)
    ids = sorted({s for j in jobs for s in j["stageIds"]})
    out = []
    for sid in ids:
        for att in get(f"/stages/{sid}"):
            if att["status"] == "COMPLETE" and att.get("shuffleReadRecords", 0) > 0:
                out.append(att)
    return [j["jobId"] for j in jobs], ids, out


print()
print(f"  prediction file: {PRED.name} ({PRED.stat().st_size} bytes), present before measuring")
print(f"  settings: master local[4], spark.sql.shuffle.partitions={spark.conf.get('spark.sql.shuffle.partitions')} (default, not changed)")
print(f"  A = {LABEL['A']}")
print(f"  B = {LABEL['B']}")
print()

warm = {s: run(s)[0] for s, _ in SETTINGS}
times = {s: [] for s, _ in SETTINGS}
results = {s: None for s, _ in SETTINGS}
last_df = {}
orders = [["A", "B"], ["B", "A"], ["A", "B"]]
for r, order in enumerate(orders):
    for s in order:
        dt, rows, df = run(s, group=f"ex06-{s}-last" if r == 2 else None)
        times[s].append(dt)
        key = sorted((x["payment_type"], x["avg_tip"]) for x in rows)
        if results[s] is None:
            results[s] = key
        elif results[s] != key:
            print(f"  WARNING: setting {s} returned different rows in round {r + 1}")
        if r == 2:
            last_df[s] = df

print(f"  round order: {' | '.join(','.join(o) for o in orders)}")
print()
print(f"  {'setting':<46}{'warm-up s':>10}{'run 1 s':>10}{'run 2 s':>10}{'run 3 s':>10}{'best s':>10}")
best = {}
for s, _ in SETTINGS:
    t = times[s]
    best[s] = min(t)
    print(f"  {s + ' ' + LABEL[s]:<46}{warm[s]:>10.3f}{t[0]:>10.3f}{t[1]:>10.3f}{t[2]:>10.3f}{best[s]:>10.3f}")
print()
print(f"  best(on) / best(off) = {best['A'] / best['B']:.2f}")
print()

for s, _ in SETTINGS:
    print(f"  result rows, setting {s} ({len(results[s])} rows):")
    for pt, avg in results[s]:
        print(f"    payment_type {pt}   avg_tip {'null' if avg is None else repr(avg)}")
same = results["A"] == results["B"]
print(f"  result rows identical between A and B (exact comparison): {'PASS' if same else 'FAIL'}")
if not same:
    diffs = [abs(a[1] - b[1]) / abs(b[1]) for a, b in zip(results["A"], results["B"])
             if a[1] is not None and b[1] not in (None, 0)]
    print(f"    keys equal: {[k for k, _ in results['A']] == [k for k, _ in results['B']]}, "
          f"max relative difference of avg_tip: {max(diffs) if diffs else 'n/a'}")
print()

# ---- plans of the last executed DataFrame of each setting --------------------
for s, conf in SETTINGS:
    df = last_df[s]
    plan = spark._jvm.PythonSQLUtils.explainString(df._jdf.queryExecution(), "formatted")
    name = "ex06_plan_aqe_on.log" if conf == "true" else "ex06_plan_aqe_off.log"
    stamp = datetime.now(timezone.utc).strftime("%a %b %d %H:%M:%S UTC %Y")
    (HERE / "runs" / name).write_text(
        f"{stamp}\n"
        f"Exercise 6, setting {s}: {LABEL[s]}\n"
        f"Plan of the DataFrame of the last scored run, taken after .collect() with explain(mode='formatted').\n\n"
        f"{plan}\n")
    lines = plan.splitlines()
    print(f"  ---- setting {s}: {LABEL[s]}   (plan written to runs/{name}) ----")
    if conf == "true":
        print(f"    isFinalPlan=true appears in the plan: {'yes' if 'isFinalPlan=true' in plan else 'no'}")
    aqe = [l for l in lines if "AQEShuffleRead" in l]
    print(f"    AQEShuffleRead appears: {'yes' if aqe else 'no'}")
    for l in aqe:
        print(f"      {l.strip()}")
    for l in lines:
        if re.search(r"\bExchange\b", l) or l.startswith("Arguments: hashpartitioning") \
                or l.startswith("Arguments: coalesced"):
            print(f"    plan line: {l.strip()}")
    jobs, stage_ids, post = post_shuffle_stages(f"ex06-{s}-last")
    print(f"    last run (job group ex06-{s}-last): jobs {jobs}, stage ids {stage_ids}")
    for st in post:
        print(f"    post-shuffle stage {st['stageId']} (attempt {st['attemptId']}): {st['numTasks']} tasks, "
              f"shuffle read records {st['shuffleReadRecords']}  [from {API}/stages]")
    if not post:
        print("    post-shuffle stage: none found in the REST API")
    print()

out = HERE / "out" / "parts" / "ex06.csv"
out.parent.mkdir(parents=True, exist_ok=True)
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["exercise", "measurement", "setting", "warmup_s", "run1_s", "run2_s", "run3_s", "best_s", "source_log"])
    for s, conf in SETTINGS:
        t = times[s]
        w.writerow(["ex06", "groupBy(payment_type).avg(tip_amount)", f"spark.sql.adaptive.enabled={conf}",
                    f"{warm[s]:.3f}", f"{t[0]:.3f}", f"{t[1]:.3f}", f"{t[2]:.3f}", f"{best[s]:.3f}", "runs/ex06_aqe.log"])
print(f"  timings written to out/parts/ex06.csv")
print()

spark.stop()
