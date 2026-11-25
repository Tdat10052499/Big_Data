"""Lab 2: build out/timings.csv.

Rows come from
  - out/parts/*.csv (written by ex06..ex10 from the same variables they print), and
  - the timing tables of runs/ex02_lines.log and runs/ex05_partitions.log, parsed here:
    every number is copied by this program from the log text, never typed.
runs/ex02_lines.log has no warm-up column (its warm-up pass was not timed), so warmup_s is
left empty for Exercise 2. Section 10 (q5_write.py) is not included: it prints single values.
Run: ~/bda/venv/bin/python src/make_timings.py
"""
import csv
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
HEADER = ["exercise", "measurement", "setting", "warmup_s", "run1_s", "run2_s", "run3_s", "best_s", "source_log"]
NUM = r"(\d+\.\d+)"
rows = []

# ---- Exercise 2: "  a filter   0.0118  0.0122  0.0093  0.0093   0 / 0 / 0"
log = "runs/ex02_lines.log"
pat = re.compile(rf"^\s+([a-e]) (.+?)\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s+\d+ / \d+ / \d+\s*$")
for line in (HERE / log).read_text().splitlines():
    m = pat.match(line)
    if m:
        rows.append(["ex02", f"line {m.group(1)}", m.group(2).strip(), "",
                     m.group(3), m.group(4), m.group(5), m.group(6), log])

# ---- Exercise 5: "                   2      1.725     0.538     0.490     0.444     0.444"
log = "runs/ex05_partitions.log"
pat = re.compile(rf"^\s+(\d+)\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}\s*$")
for line in (HERE / log).read_text().splitlines():
    m = pat.match(line)
    if m:
        rows.append(["ex05", "wide aggregation groupBy(PULocationID,DOLocationID,pickup_hour), AQE off",
                     f"spark.sql.shuffle.partitions={m.group(1)}",
                     m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), log])

# ---- out/parts/*.csv
for part in sorted((HERE / "out" / "parts").glob("*.csv")):
    with part.open(newline="") as f:
        r = csv.reader(f)
        if next(r) != HEADER:
            raise SystemExit(f"STOP: unexpected header in {part}")
        rows.extend(r)

rows.sort(key=lambda x: x[0])           # stable: keeps each exercise's own order
out = HERE / "out" / "timings.csv"
with out.open("w", newline="") as f:
    w = csv.writer(f)
    w.writerow(HEADER)
    w.writerows(rows)
counts = {}
for x in rows:
    counts[x[0]] = counts.get(x[0], 0) + 1
print(f"wrote {out.relative_to(HERE)}: {len(rows)} rows " + ", ".join(f"{k} {v}" for k, v in counts.items()))
