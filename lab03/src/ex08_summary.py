"""Lab 3, Exercise 8 summary: reads out/latency_parts/ex08_t*.csv and the producer logs.
Run:  ~/bda/venv/bin/python src/ex08_summary.py
"""
import csv, re, statistics
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
print("  Exercise 8 summary, topic taxi-live, producer requested 1,500 events per second")
print("  the 8 batches = the first 8 non-empty batches in each CSV, in batch order")
for trig in [1, 3, 10]:
    tag = f"ex08_t{trig}"
    rows = list(csv.DictReader(open(BASE / "out" / "latency_parts" / f"{tag}.csv")))
    topic = rows[0]["topic"]
    eight = sorted(rows, key=lambda r: int(r["batch"]))[:8]
    n = [int(r["rows"]) for r in eight]
    lag = [float(r["avg_lag_s"]) for r in eight]
    lagp = [float(r["avg_lag_s_precise"]) for r in eight]
    plog = (BASE / "runs" / f"{tag}_producer.log").read_text()
    ach = re.search(r"achieved rate\s+([\d.]+) events per second", plog).group(1)
    sent = re.search(r"events sent\s+([\d,]+)", plog).group(1)
    print()
    print(f"  trigger {trig} s ({tag}), topic {topic}: {len(rows)} non-empty batches in the CSV, 8 used")
    print(f"    {'batch':>5} {'rows':>7} {'rows/s in':>10} {'rows/s out':>11} {'trigger ms':>10} {'avg lag s':>9} {'avg lag s precise':>18}")
    for r in eight:
        print(f"    {r['batch']:>5} {int(r['rows']):>7,} {float(r['input_rows_per_s']):>10.1f} "
              f"{float(r['processed_rows_per_s']):>11.1f} {int(r['trigger_ms']):>10,} {r['avg_lag_s']:>9} "
              f"{float(r['avg_lag_s_precise']):>18.3f}   topic {topic}")
    print(f"    mean rows per batch, topic {topic}, trigger {trig} s:   {statistics.mean(n):,.1f}")
    print(f"    median rows per batch, topic {topic}, trigger {trig} s: {statistics.median(n):,.1f}")
    print(f"    mean of avg_lag_s, topic {topic}, trigger {trig} s:          {statistics.mean(lag):.3f}")
    print(f"    mean of avg_lag_s_precise, topic {topic}, trigger {trig} s:  {statistics.mean(lagp):.3f}")
    print(f"    precise lag first-to-last change (batch {eight[-1]['batch']} minus batch {eight[0]['batch']}), "
          f"topic {topic}, trigger {trig} s: {lagp[-1] - lagp[0]:+.3f} s")
    print(f"    producer ({tag}_producer.log), topic {topic}: events sent {sent}, achieved rate {ach} events per second")
print()
