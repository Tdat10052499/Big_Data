# Rules for Claude Code working in lab03/ on the student's machine

Student: Hồ Du Tuấn Đạt, 2374802010097, Linux/HDFS user `tdat1`, WSL Ubuntu, Spark 3.5.9, Hadoop 3.4.3, Java 17, Python 3.14.4 (venv `~/bda/venv`). Lab 1 and Lab 2 are finished: never modify `lab01/` or `lab02/`.

1. Never invent, edit, round or retype a number. Every number comes from a command run in this session. Logs are never edited after they are written; images are rendered from logs only.
2. Every log: `runs/<name>.log` = stdout with `date -u` on the first line; Spark stderr in `runs/<name>.err`. Pattern: `{ date -u; <command> 2>runs/<name>.err; } | tee runs/<name>.log`. Producer runs: `{ date -u; <producer command>; } 2>&1 | tee runs/<name>_producer.log`. Background runs write the same way through `( ... ) &`.
3. Images: `~/bda/venv/bin/python src/log_to_png.py runs/<name>.log figures/lab03_<name>.png` (`src/log_to_png.py` is a copy of `lab02/src/log_to_png.py`, unchanged). Never draw or retouch an image.
4. Every count names its topic. Topics: taxi-trips, taxi-late-demo, taxi-restart, taxi-live, taxi-ex1. Every script prints the topic name on the line of every count it prints. Every percentage names its cohort (e.g. "15.04% of the 20,000 events sent to taxi-trips").
5. Predictions (Exercises 5-8) are the student's work. Never write, suggest, draft, complete, correct, rephrase or translate a prediction, and never hint at the outcome. Ask the exact question, save the text verbatim to `runs/exNN_prediction.txt` (or the no-prediction text if the student types skip), confirm, render, commit and push BEFORE measuring. Never modify it afterwards. Never compare the result with the prediction.
6. Spark command, run from `~/bda/lab03`: `SS="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 --py-files src/s0_common.py"`. Every script uses `master("local[4]")` (via `s0_common.spark_session`). Producer: always `~/bda/venv/bin/python src/trip_producer.py ...`.
7. Checkpoint and output paths are always full `hdfs://localhost:9000/user/tdat1/nyc/stream/...` URIs. Never delete anything under `/user/tdat1/nyc` except the stream directories the lab scripts themselves reset (`nyc/stream/ck/*`, `nyc/stream/<query>`). Never delete raw, ref, curated, curated_q1, curated_jan_feb, agg.
8. Never kill, restart or reformat the Kafka broker or HDFS. `pkill` only on the exact patterns given in a step (`s3_restart.py`, `ex10_repeat.py`, `trip_producer.py`).
9. If HDFS refuses connections or Kafka is not UP, stop and report. If a Spark run fails, save the output, show the last 40 lines of the .err file, and stop; at most one re-run after a fix to the script, and say what was changed.
10. Git: only branch `claude/lab03-streaming` (from origin/main). Plain `git push -u origin claude/lab03-streaming`; no force-push, merge, rebase, or changes to `.github/`. Commit only the named files (never `git add -A`). Commit and push after each numbered step.
11. Python 3.14: no `toPandas()`; use `.collect()`.
12. At the end of every step print the log(s) verbatim. Do not summarise or interpret numbers. Do not write answers.md, runs/run_record.md or any report.
