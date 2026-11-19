# Prompt for Claude Code on the laptop's WSL: Exercises 5-8 (predictions first)

Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude` as user `tdat1`.

---

You are working in `~/Big_Data` on the student's laptop (WSL Ubuntu, user `tdat1`). First read `lab04/CLAUDE.md` and follow it strictly.

Goal: Lab 4 Exercises 5, 6, 7 and 8. Each one asks the student for a prediction BEFORE the measurement. Push everything to branch `claude/lab04-storage-b5z7je`.

IMPORTANT, for all four exercises:
- You must NOT write, suggest, draft, complete, correct, rephrase or translate a prediction, and you must not hint at what the result will be, not even by saying which numbers are "relevant". You may show the facts listed in each "Facts" step and nothing more.
- When you ask, use exactly the question given, then STOP and wait for my answer.
- Save my answer exactly as I typed it, show it back, then commit and push it BEFORE running the measurement. Never edit a prediction file afterwards.
- If a measurement fails, save the failing output, report it, and do not change the prediction.

Every Bash call may start a fresh shell, so put `source ~/bda/lab04/env.sh &&` in front of every command. Use a timeout of at least 10 minutes for Spark steps.

How to save a prediction (replace NN and the text):
```bash
cd ~/bda/lab04 && { echo "Exercise NN prediction, written by the student before measuring."; echo "Recorded: $(date -u)"; echo "---"; cat <<'PRED'
THE STUDENT TEXT, VERBATIM
PRED
} > runs/exNN_prediction.txt && cat runs/exNN_prediction.txt
cd ~/Big_Data && git add lab04/runs/exNN_prediction.txt && git commit -m "Lab 4: Exercise NN prediction (before measuring)" && git push origin claude/lab04-storage-b5z7je
```

Step 0. Update and start.
```bash
cd ~/Big_Data && git status --short && git pull origin claude/lab04-storage-b5z7je
bash ~/bda/lab04/start_cluster.sh
```
If `git status --short` shows changes, or start_cluster prints STOP, stop and show me.

## Exercise 5: predict the bytes before you read them

Step 5a. Facts. Show me this output, and remind me that the Section 4 bytes are in `lab04/runs/sec4_plan.log`.
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- the three January Parquet files"; hdfs dfs -ls /user/tdat1/nyc/fmt/2024-01/parquet; echo "--- the three January CSV files"; hdfs dfs -ls /user/tdat1/nyc/fmt/2024-01/csv; [ -f out/sample.parquet ] || hdfs dfs -get -f $(hdfs dfs -ls /user/tdat1/nyc/fmt/2024-01/parquet | sort -k5 -n | tail -1 | awk '{print $8}') out/sample.parquet; python src/f2_footer.py out/sample.parquet fare_amount tip_amount payment_type; } 2>&1 | tee runs/ex05_facts.log
```

Step 5b. Ask exactly: "Exercise 5: using the footer above, predict how many bytes Parquet will read for (1) the sum of tip_amount and (2) the average fare_amount for each payment_type, and how many bytes CSV will read for each. The footer covers one file of three: say how you scaled it. I will save your text exactly as you type it." Then STOP and wait. Save it as `runs/ex05_prediction.txt`, commit and push.

Step 5c. Measure (the two queries are already added to the `queries` list in `src/f1_formats.py`).
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/f1_formats.py 2>runs/ex05_measure.err; } | tee runs/ex05_measure.log
```

## Exercise 6: predict a MERGE

Step 6a. Start again from version 0, then the facts.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/d1_create.py 2>runs/ex06_reset.err; echo "--- files in the table"; hdfs dfs -ls $T; echo "--- which correction zones the table already has (bda.zone_summary holds the same 258 rows)"; mongosh bda --quiet --eval 'db.zone_summary.find({zone_id: {$in: [132,161,237,236,103,104,109]}}, {_id: 0, zone_id: 1}).sort({zone_id: 1}).toArray()'; echo "--- the corrections in src/e6_merge.py"; sed -n '/corrections = spark.createDataFrame/,/], schema)/p' src/e6_merge.py; } 2>&1 | tee runs/ex06_facts.log
```
If the zone list does not show exactly 132, 161, 236 and 237, stop and tell me (the exercise needs four zones that exist).

Step 6b. Ask exactly: "Exercise 6: before the MERGE runs, predict the new version number, the new row count, and the six metrics the script prints: numSourceRows, numTargetRowsUpdated, numTargetRowsInserted, numTargetRowsCopied, numTargetFilesRemoved and numTargetFilesAdded. I will save your text exactly as you type it." Then STOP and wait. Save as `runs/ex06_prediction.txt`, commit and push.

Step 6c. Measure, then put the table back the way the manual leaves it (version 0 + the Section 5 MERGE), because the final project keeps `lake/zone_summary`.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/e6_merge.py 2>runs/ex06_merge.err; echo "--- files in the table"; hdfs dfs -ls $T; } | tee runs/ex06_merge.log
source ~/bda/lab04/env.sh && { date -u; $SS src/d1_create.py 2>runs/ex06_restore.err; $SS src/d2_merge.py 2>>runs/ex06_restore.err; } | tee runs/ex06_restore.log
```

## Exercise 7: find where the index pays

Step 7a. Facts.
```bash
source ~/bda/lab04/env.sh && { date -u; mongosh bda --quiet --eval 'print("documents " + db.trips_jan.countDocuments({})); printjson(db.trips_jan.getIndexes().map(i => i.key)); printjson(db.trips_jan.findOne({}, {PULocationID: 1, pickup_date: 1}))'; echo "--- the three queries"; sed -n '/const queries/,/];/p' src/e7_index.js; } 2>&1 | tee runs/ex07_facts.log
```

Step 7b. Ask exactly: "Exercise 7: before the index is created, predict which of the three queries (zone 132; zone 132 on 15 January; all trips on 15 January) will use the index {PULocationID: 1, pickup_date: 1}, and why. I will save your text exactly as you type it." Then STOP and wait. Save as `runs/ex07_prediction.txt`, commit and push.

Step 7c. Measure: no index, the compound index, then an extra index for the third query, and the index sizes.
```bash
source ~/bda/lab04/env.sh && { date -u; mongosh bda --quiet src/e7_index.js; } 2>&1 | tee runs/ex07_index.log
```

## Exercise 8: append instead of overwrite

Step 8a. Facts.
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- what the nightly job does now"; sed -n '/^def nightly_job/,/done.set/p' src/e8_append.py; echo "--- the check"; grep -n "sizes = " src/e8_append.py; } 2>&1 | tee runs/ex08_facts.log
```

Step 8b. Ask exactly: "Exercise 8: predict what the dashboard will see while the short trips are appended six times, with Parquet and with Delta. I will save your text exactly as you type it." Then STOP and wait. Save as `runs/ex08_prediction.txt`, commit and push.

Step 8c. The two default runs.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/e8_append.py parquet 2>runs/ex08_parquet.err; } | tee runs/ex08_parquet.log
source ~/bda/lab04/env.sh && { date -u; $SS src/e8_append.py delta 2>runs/ex08_delta.err; } | tee runs/ex08_delta.log
```

Step 8d. Ask exactly: "Exercise 8, optional: before the runs where every append writes 64 files, do you want to add a second prediction? Type it, or type 'no'." Then STOP and wait. If I type a prediction, save it as `runs/ex08_prediction_64.txt` the same way (commit and push first). If I type 'no', save nothing.

Step 8e. The two runs with 64 files per append.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/e8_append.py parquet 64 2>runs/ex08_parquet64.err; } | tee runs/ex08_parquet64.log
source ~/bda/lab04/env.sh && { date -u; $SS src/e8_append.py delta 64 2>runs/ex08_delta64.err; } | tee runs/ex08_delta64.log
```

## Finish

Step 9. Render every log to an image.
```bash
cd ~/bda/lab04 && for n in ex05_facts ex05_measure ex06_facts ex06_merge ex06_restore ex07_facts ex07_index ex08_facts ex08_parquet ex08_delta ex08_parquet64 ex08_delta64; do ~/bda/venv/bin/python src/log_to_png.py runs/$n.log figures/lab04_$n.png; done
```
Also render each prediction file the same way (`runs/exNN_prediction.txt` -> `figures/lab04_exNN_prediction.png`).

Step 10. Self-check, reading only the files (change nothing). PASS or FAIL:
- each `exNN_prediction.txt` was committed BEFORE its measurement log (compare `git log --format='%h %ci %s' -- lab04/runs/exNN_prediction.txt` with the first line of the measurement log);
- `ex05_measure.log` has five query rows, each with CSV and Parquet bytes and `[timing]` lines;
- `ex06_merge.log` prints version, the six metrics, the data files before and after, and the row count; `ex06_restore.log` ends with 260 rows;
- `ex07_index.log` has 3 queries x 3 index states with the four numbers, and an index size list;
- each of the four `ex08_*.log` prints the k = 0..6 table and the counts of other reads.

Step 11. Commit and push.
```bash
cd ~/Big_Data && git add lab04/runs lab04/figures && git status --short && git commit -m "Lab 4: Exercises 5-8 measurements" && git push origin claude/lab04-storage-b5z7je
```

Finally print, verbatim: every `exNN_prediction*.txt`, `cat` of the twelve logs from Step 9, the self-check table, and `git log -8 --oneline`.
