#!/usr/bin/env bash
# Build the checkpoint archive (manual section 13). Datasets in HDFS and out/*.parquet are not included.
# Run: bash ~/bda/lab04/make_zip.sh
set -e
cd ~/Big_Data/lab04
for f in runs/run_record.md out/ADR-001.md out/loadtest.csv answers.md; do
  [ -f "$f" ] || { echo "missing $f"; exit 1; }
done
rm -f ~/lab04_2374802010097.zip
zip -r ~/lab04_2374802010097.zip src runs out answers.md figures -x '*__pycache__*' -x 'out/*.parquet'
unzip -l ~/lab04_2374802010097.zip
