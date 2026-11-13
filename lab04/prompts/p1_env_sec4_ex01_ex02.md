# Prompt for Claude Code on the student's WSL: Phase 0, Section 4, Exercises 1 and 2

Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude`.

---

You are working in `~/Big_Data` on the student's own WSL Ubuntu machine. First read `lab04/CLAUDE.md` and follow it strictly: never invent or edit numbers, never write predictions, never delete HDFS data under `/user/tdat1/nyc`, never touch `lab01/` or `lab02/`, never force-push, no `sudo` without asking me.

Goal: real evidence for Lab 4 Phase 0 (manual sections 2-3), Section 4, Exercise 1 and Exercise 2, pushed to branch `claude/lab04-storage-b5z7je`.

Step 1. Branch and link. If `git status --short` shows uncommitted changes, stop and show them to me before checking out.
```bash
cd ~/Big_Data && git status --short && git fetch origin claude/lab04-storage-b5z7je && git checkout claude/lab04-storage-b5z7je && git pull origin claude/lab04-storage-b5z7je
bash lab04/setup_lab04.sh
~/bda/venv/bin/python -c "import PIL; print('Pillow', PIL.__version__)" || ~/bda/venv/bin/pip install pillow
```

Step 2. Check the three tools this lab adds (manual 2.1). Run each line and show me the output:
```bash
mongod --version | head -1; mongosh --version; jq --version; ~/bda/venv/bin/python -c "import pymongo; print(pymongo.version)"
```
If any of them answers `command not found` or `No module named 'pymongo'`, STOP and show me the output. Do not install anything until I say yes. (The install commands are in manual 2.1; they need `sudo`.)

Step 3. Start the cluster and MongoDB, then set the session variables. If start_cluster prints STOP, stop and show me the output.
```bash
bash ~/bda/lab04/start_cluster.sh
source ~/bda/lab04/env.sh
```
Note: every Bash call may start a fresh shell. Put `source ~/bda/lab04/env.sh &&` in front of every later command that uses `$SS`, `$T` or `python`.

Step 4. Phase 0 environment evidence.
```bash
source ~/bda/lab04/env.sh && { date -u; bda-status.sh; echo "--- versions"; mongod --version | head -1; mongosh --version; jq --version; python -c "import pymongo, pyarrow; print('pymongo', pymongo.version); print('pyarrow', pyarrow.__version__)"; spark-submit --version 2>&1 | grep -m1 version; hadoop version | head -1; java -version 2>&1 | head -1; python --version; } 2>&1 | tee runs/ex00_env.log
```

Step 5. Inputs from Labs 1-3 (manual 2.3 and 3).
```bash
source ~/bda/lab04/env.sh && { date -u; N=/user/tdat1/nyc; echo "--- agg"; hdfs dfs -ls -C $N/agg; echo "--- stream/ck"; hdfs dfs -ls -C $N/stream/ck; echo "--- count (dirs files bytes path)"; hdfs dfs -count $N/curated $N/curated_q1 $N/ref/taxi_zone_lookup.csv $N/agg/zone_summary $N/agg/zone_hour; echo "--- section 3: du"; hdfs dfs -du -s -h $N/agg/zone_summary $N/curated; } 2>&1 | tee runs/sec2_inputs.log
```
Then read the log. If `agg/zone_summary` or `agg/zone_hour` is missing, STOP and tell me (they come from Lab 2 `q5_write.py`; do not run it without my yes). If `stream/ck/restart` is missing, do NOT stop: write `stream/ck/restart missing (Lab 3 section 10 not run on this machine)` in your final report and continue.

Step 6. Section 4.1: write January twice, then the sizes.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/f1_formats.py --write 2>runs/sec4_write.err; hdfs dfs -du -s -h /user/tdat1/nyc/fmt/2024-01/csv /user/tdat1/nyc/fmt/2024-01/parquet; } | tee runs/sec4_write.log
```
If the log has no `wrote ... rows` line, show me the last 40 lines of `runs/sec4_write.err` and stop.

Step 7. Section 4.2: measure the three queries and print the plans.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/f1_formats.py --plan 2>runs/sec4_plan.err; } | tee runs/sec4_plan.log
```

Step 8. Section 4.3: the footer of the largest January Parquet file.
```bash
source ~/bda/lab04/env.sh && { date -u; F=$(hdfs dfs -ls /user/tdat1/nyc/fmt/2024-01/parquet | sort -k5 -n | tail -1 | awk '{print $8}'); echo "largest file: $F"; hdfs dfs -ls /user/tdat1/nyc/fmt/2024-01/parquet; hdfs dfs -get -f $F out/sample.parquet; python src/f2_footer.py out/sample.parquet; } 2>&1 | tee runs/sec4_footer.log
```

Step 9. Exercise 1: February.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/f1_formats.py --write --month 2024-02 2>runs/ex01_write.err; hdfs dfs -du -s -h /user/tdat1/nyc/fmt/2024-02/csv /user/tdat1/nyc/fmt/2024-02/parquet; } | tee runs/ex01_write.log
source ~/bda/lab04/env.sh && { date -u; $SS src/f1_formats.py --month 2024-02 2>runs/ex01_measure.err; } | tee runs/ex01_measure.log
```

Step 10. Exercise 2: the footer of Lab 2's `agg/zone_hour`.
```bash
source ~/bda/lab04/env.sh && { date -u; hdfs dfs -ls /user/tdat1/nyc/agg/zone_hour; hdfs dfs -get -f /user/tdat1/nyc/agg/zone_hour/part-*.parquet out/zone_hour.parquet; python src/f2_footer.py out/zone_hour.parquet PULocationID pickup_hour zone_hour_avg; } 2>&1 | tee runs/ex02_footer.log
```
If `-get` complains that the destination must be a directory (more than one part file), stop and show me the `-ls` output.

Step 11. Render every log to an image.
```bash
cd ~/bda/lab04 && for n in ex00_env sec2_inputs sec4_write sec4_plan sec4_footer ex01_write ex01_measure ex02_footer; do ~/bda/venv/bin/python src/log_to_png.py runs/$n.log figures/lab04_$n.png; done
```

Step 12. Self-check, reading only the logs (do not change anything). Report each as PASS or FAIL with the numbers copied from the log:
- `sec4_write.log`: rows written for 2024-01 equals 2,724,143 (manual value);
- `sec4_plan.log`: there are 6 `[timing]` lines, each with a warm-up and three runs; for "sum one column" Parquet read fewer bytes than CSV; both scan lines end in `ReadSchema: struct<fare_amount:double>`;
- `sec4_footer.log`: the file named on the `largest file:` line is the largest in the listing;
- `ex01_write.log`: a rows line for 2024-02 exists; `ex01_measure.log` has the stored sizes, the ratio and three query rows;
- `ex02_footer.log`: the three columns PULocationID, pickup_hour, zone_hour_avg each have a min and a max.

Step 13. Commit and push only the evidence (`out/*.parquet` is ignored on purpose).
```bash
cd ~/Big_Data && git add lab04/runs lab04/figures && git status --short && git commit -m "Lab 4: Phase 0, Section 4, Exercises 1 and 2 evidence" && git push origin claude/lab04-storage-b5z7je
```

Finally print, verbatim: `cat` of the eight logs from Step 11, the self-check table, any `stream/ck/restart missing` note, and `git log -1 --oneline`.
