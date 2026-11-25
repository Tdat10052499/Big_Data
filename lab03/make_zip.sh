#!/usr/bin/env bash
# Build the Lab 3 submission archive ~/lab03_2374802010097.zip from src runs out answers.md figures.
set -eu
cd "$(dirname "$0")"
for f in runs/run_record.md out/latency.csv answers.md; do
  [ -e "$f" ] || { echo "REFUSED: $f is missing"; exit 1; }
done
ZIP=~/lab03_2374802010097.zip
rm -f "$ZIP"
~/bda/venv/bin/python - "$ZIP" <<'PY'
import os, sys, zipfile
with zipfile.ZipFile(sys.argv[1], "w", zipfile.ZIP_DEFLATED) as z:
    for top in ["src", "runs", "out", "answers.md", "figures"]:
        if os.path.isfile(top):
            z.write(top, f"lab03/{top}")
            continue
        for d, _, files in os.walk(top):
            if "__pycache__" in d:
                continue
            for f in sorted(files):
                p = os.path.join(d, f)
                z.write(p, f"lab03/{p}")
print("wrote", sys.argv[1])
PY
unzip -l "$ZIP" | tail -1
