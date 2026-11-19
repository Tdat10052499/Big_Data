"""Lab 4: paths and helpers shared by every Spark script in this lab.

Import it rather than copying the paths into eight files.
    from l0_common import spark_session, NYC, AGG, LAKE, FMT, MONGO

The Delta and MongoDB libraries are not part of Spark. Spark has to load them
before a session exists, so they come from the spark-submit command line with
--packages, through the $SS variable set up in Section 2 of the manual. Only
the settings that switch them on live here.
"""
import json
import statistics
import time
import urllib.request

from pyspark.sql import SparkSession

HDFS = "hdfs://localhost:9000"
NYC = f"{HDFS}/user/tdat1/nyc"
AGG = f"{NYC}/agg"          # written by Lab 2
LAKE = f"{NYC}/lake"        # written by this lab
FMT = f"{NYC}/fmt"          # the format comparison in Section 4
MONGO = "mongodb://127.0.0.1:27017"


def spark_session(name):
    spark = (SparkSession.builder
             .appName(name)
             .master("local[4]")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog",
                     "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .config("spark.mongodb.read.connection.uri", MONGO)
             .config("spark.mongodb.write.connection.uri", MONGO)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def hdfs_bytes(path):
    """Bytes stored under an HDFS path, as hdfs dfs -du -s reports them."""
    import subprocess
    out = subprocess.run(["/opt/hadoop/bin/hdfs", "dfs", "-du", "-s", path],
                         capture_output=True, text=True).stdout.split()
    return int(out[0]) if out else 0


def bytes_read(spark, action):
    """Run action() and return (result, bytes Spark read from storage).

    The number comes from Spark's own task metrics, the same figure the web
    interface shows as Input for each stage. It counts the bytes the readers
    actually pulled from HDFS, so a column that was never read adds nothing.
    """
    sc = spark.sparkContext
    group = f"measure-{time.time_ns()}"
    sc.setJobGroup(group, group)
    result = action()
    tracker = sc.statusTracker()
    stage_ids = []
    for job_id in tracker.getJobIdsForGroup(group):
        info = tracker.getJobInfo(job_id)
        if info:
            stage_ids.extend(info.stageIds)
    base = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/stages"
    total = 0
    for sid in stage_ids:
        for _ in range(40):                     # the listener can lag a moment
            try:
                with urllib.request.urlopen(f"{base}/{sid}") as r:
                    attempts = json.load(r)
                if all(a.get("status") in ("COMPLETE", "SKIPPED", "FAILED") for a in attempts):
                    total += sum(a.get("inputBytes", 0) for a in attempts
                                 if a.get("status") == "COMPLETE")
                    break
            except Exception:
                pass
            time.sleep(0.05)
    return result, total


def best_of_three(action, label=None):
    """The course convention: one warm-up run, then the fastest of three.

    Student change: the warm-up and all three scored runs are also printed,
    one line per call, so the run record can quote every run, not only the best.
    """
    t0 = time.perf_counter()
    action()
    warm = time.perf_counter() - t0
    runs = []
    for _ in range(3):
        t0 = time.perf_counter()
        action()
        runs.append(time.perf_counter() - t0)
    if label:
        r = " / ".join(f"{x:.3f}" for x in runs)
        print(f"    [timing] {label:<34} warm-up {warm:.3f} s | runs {r} s | best {min(runs):.3f} s")
    return min(runs)


def latency_ms(action, n):
    """Run action() n times. Return the median and the 95th percentile, in ms."""
    action()
    runs = []
    for _ in range(n):
        t0 = time.perf_counter()
        action()
        runs.append((time.perf_counter() - t0) * 1000)
    runs.sort()
    return statistics.median(runs), runs[min(n - 1, int(round(0.95 * n)) - 1)]


def mb(n):
    """Binary megabytes, the unit hdfs dfs -du -h prints as M."""
    if n < 1024 * 1024:
        return f"{n / 1024:,.0f} KB"
    return f"{n / (1024 * 1024):,.1f} MB"
