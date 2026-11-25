"""Lab 3: concatenate out/latency_parts/*.csv into out/latency.csv (header once, values copied)."""
import csv
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
parts = sorted((BASE / "out" / "latency_parts").glob("*.csv"))
header, rows = None, []
for p in parts:
    with open(p, newline="") as fh:
        r = csv.reader(fh)
        h = next(r)
        if header is None:
            header = h
        assert h == header, f"header differs in {p.name}"
        body = list(r)
        rows += body
        print(f"  {p.name}: {len(body)} rows (topic {body[0][1] if body else '-'})")
with open(BASE / "out" / "latency.csv", "w", newline="") as fh:
    w = csv.writer(fh)
    w.writerow(header)
    w.writerows(rows)
print(f"  wrote out/latency.csv: {len(rows)} rows from {len(parts)} files")
print()
print((BASE / "out" / "latency.csv").read_text(), end="")
