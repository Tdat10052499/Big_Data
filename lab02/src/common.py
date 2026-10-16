"""Shared paths and timing helpers for the Lab 2 exercise scripts (ex01..ex10).

Paths use the HDFS account tdat1; the manual hardcodes /user/thaianh/.
Timing rule for every measurement: one warm-up run, then three scored runs.
All three scored runs are reported, together with the best (the minimum).
"""
import time

HDFS = "hdfs://localhost:9000"
NYC = f"{HDFS}/user/tdat1/nyc"
RAW = f"{NYC}/raw"
REF_ZONES = f"{NYC}/ref/taxi_zone_lookup.csv"
CURATED = f"{NYC}/curated"            # Lab 1, January only
CURATED_Q1 = f"{NYC}/curated_q1"      # Section 3, January to March
CURATED_JAN_FEB = f"{NYC}/curated_jan_feb"   # Exercise 1


def timed(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


def warm_then_three(fn):
    """Run fn once as a warm-up (not scored), then three scored runs.
    Returns (warmup_s, [run1_s, run2_s, run3_s], best_s)."""
    warm = timed(fn)
    runs = [timed(fn) for _ in range(3)]
    return warm, runs, min(runs)


def fmt_runs(label, warm, runs, best):
    r = " / ".join(f"{x:.3f}" for x in runs)
    return f"  {label:<34} warm-up {warm:.3f} s | runs {r} s | best {best:.3f} s"
