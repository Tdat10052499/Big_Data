"""Lab 3, Exercise 10 summary: prints runs/ex10_side_effects.txt verbatim, then every batch_id
that appears more than once, with its pids and times.
Run:  ~/bda/venv/bin/python src/ex10_summary.py
"""
import re
from collections import defaultdict
from pathlib import Path

SIDE = Path(__file__).resolve().parent.parent / "runs" / "ex10_side_effects.txt"
text = SIDE.read_text()
print(f"  --- {SIDE.name} (verbatim; topic taxi-restart) ---")
print(text, end="")
print("  --- end of file ---")
seen = defaultdict(list)
for line in text.splitlines():
    m = re.match(r"(\S+ \S+) batch_id=(\d+) rows=(\d+) pid=(\d+)", line)
    if m:
        seen[int(m.group(2))].append((m.group(1), m.group(4), m.group(3)))
print(f"  side-effect lines: {sum(len(v) for v in seen.values())}; distinct batch_ids: {len(seen)}; topic taxi-restart")
dups = {b: v for b, v in sorted(seen.items()) if len(v) > 1}
if not dups:
    print("  batch_ids that appear more than once: none")
for b, v in dups.items():
    print(f"  batch_id={b} appears {len(v)} times (topic taxi-restart):")
    for t, pid, rows in v:
        print(f"    {t} UTC  pid={pid}  rows={rows}")
