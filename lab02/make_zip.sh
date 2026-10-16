#!/usr/bin/env bash
# Build the checkpoint archive (manual section 13). Datasets in HDFS are not included.
# Run: bash ~/bda/lab02/make_zip.sh
set -e
cd ~/Big_Data/lab02
for f in runs/run_record.md out/timings.csv answers.md; do
  [ -f "$f" ] || { echo "missing $f"; exit 1; }
done
rm -f ~/lab02_2374802010097.zip
zip -r ~/lab02_2374802010097.zip src runs out answers.md figures -x '*__pycache__*'
unzip -l ~/lab02_2374802010097.zip
