# Prompt for Claude Code on the laptop's WSL: Sections 6, 7, 8, 9 and 10.3

Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude` as user `tdat1`.

---

You are working in `~/Big_Data` on the student's laptop (WSL Ubuntu, user `tdat1`). First read `lab04/CLAUDE.md` and follow it strictly: never invent or edit numbers, never write predictions, never touch `lab01/` or `lab02/`, never delete HDFS data under `/user/tdat1/nyc` except what the lab scripts themselves overwrite (`lake/readers_*`, MongoDB `bda.*`), never force-push, no `sudo`.

Goal: real evidence for manual Sections 6 (readers during a rewrite), 7 (checkpoint vs Delta log), 8 (MongoDB), 9 (both questions on every store) and 10.3 (what the data reveals), pushed to branch `claude/lab04-storage-b5z7je`. No predictions are needed in this prompt.

Every Bash call may start a fresh shell, so put `source ~/bda/lab04/env.sh &&` in front of every command. Several steps take minutes; use a timeout of at least 10 minutes per step and do not interrupt them.

Step 1. Update and start.
```bash
cd ~/Big_Data && git status --short && git pull origin claude/lab04-storage-b5z7je
bash ~/bda/lab04/start_cluster.sh
df -h ~ | tail -1
```
If `git status --short` shows changes, or start_cluster prints STOP, stop and show me.

Step 2. Section 6: a dashboard reading while a job rewrites the table, plain Parquet then Delta.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/d4_readers.py parquet 2>runs/sec6_parquet.err; } | tee runs/sec6_parquet.log
source ~/bda/lab04/env.sh && { date -u; $SS src/d4_readers.py delta 2>runs/sec6_delta.err; } | tee runs/sec6_delta.log
```
Run each ONCE. The manual says a Parquet run may by chance show only whole tables; if that happens, do not re-run on your own, just report it to me.

Step 3. Section 7: the two logs side by side. `stream/ck/restart` (Lab 3) is expected to be missing on this laptop; keep the error line in the log.
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- Lab 3 streaming checkpoint"; hdfs dfs -ls -C /user/tdat1/nyc/stream/ck/restart; echo "--- Delta log of lake/zone_summary"; hdfs dfs -ls -C $T/_delta_log; } 2>&1 | tee runs/sec7_logs.log
```

Step 4. Section 8.1-8.2: load MongoDB, then one shaped document.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/m1_load.py 2>runs/sec8_load.err; echo "--- findOne zone 132, first 3 hours"; mongosh bda --quiet --eval 'db.zone_profile.findOne({zone_id: 132}, {hours: {$slice: 3}})'; } 2>&1 | tee runs/sec8_load.log
```
(stderr of spark-submit goes to the .err file; the `2>&1` after the braces only catches mongosh.)

Step 5. Section 8.3: what an index changes (plain Python, not $SS).
```bash
source ~/bda/lab04/env.sh && { date -u; python src/m2_lookup.py; } 2>&1 | tee runs/sec8_lookup.log
```

Step 6. Section 9.1: a month of trips into MongoDB.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/m3_trips.py 2>runs/sec9_trips.err; } | tee runs/sec9_trips.log
```

Step 7. Section 9.2: Q1 and Q2 on every store.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/s1_compare.py 2>runs/sec9_compare.err; } | tee runs/sec9_compare.log
```

Step 8. Section 10.3: what MongoDB says about itself, and what the data reveals. The manual types these at the `bda>` prompt; run them with `--eval` so they can be logged.
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- startup warnings"; mongosh bda --quiet --eval 'db.adminCommand({getLog: "startupWarnings"}).log.forEach(l => print(JSON.parse(l).msg))'; echo "--- server and shell versions"; mongosh bda --quiet --eval 'print("Using MongoDB: " + db.version())'; mongosh --version; echo "--- zones with fewer than 10 trips"; mongosh bda --quiet --eval 'db.zone_summary.countDocuments({trips: {$lt: 10}})'; echo "--- zones with exactly 1 trip"; mongosh bda --quiet --eval 'db.zone_summary.find({trips: 1}, {_id: 0, trips: 0}).sort({zone_id: 1}).toArray()'; echo "--- total zones"; mongosh bda --quiet --eval 'db.zone_summary.countDocuments({})'; } 2>&1 | tee runs/sec10_reveal.log
```

Step 9. Render every log to an image.
```bash
cd ~/bda/lab04 && for n in sec6_parquet sec6_delta sec7_logs sec8_load sec8_lookup sec9_trips sec9_compare sec10_reveal; do ~/bda/venv/bin/python src/log_to_png.py runs/$n.log figures/lab04_$n.png; done
```

Step 10. Self-check, reading only the logs (change nothing). PASS or FAIL with numbers copied from the logs:
- `sec6_parquet.log` and `sec6_delta.log`: each prints the two whole sizes (2,724,143 and 1,596,003 in the manual), the number of reads, and how many saw a whole table;
- `sec7_logs.log`: the Delta log listing shows commits 00..00.json and 00..01.json;
- `sec8_load.log`: zone_summary written 258 / read back 258; the column list after has one more column (`_id`) than before; zone_profile written 258; the findOne shows 3 hours;
- `sec8_lookup.log`: plan without index is COLLSCAN with docs 258; with index FETCH > IXSCAN with keys 1, docs 1; the two median / 95th percentile lines are printed;
- `sec9_trips.log`: documents written 2,724,143; four size lines;
- `sec9_compare.log`: three Q1 lines (median, 95th pct), two Q2 lines with `[timing]` lines above them, and the agreement line;
- `sec10_reveal.log`: the access-control warning is among the startup warnings; the count of zones under 10 trips and the list of 1-trip zones are printed.

Step 11. Commit and push.
```bash
cd ~/Big_Data && git add lab04/runs lab04/figures && git status --short && git commit -m "Lab 4: Sections 6-9 and 10.3 evidence" && git push origin claude/lab04-storage-b5z7je
```

Finally print, verbatim: `cat` of the eight logs from Step 9, the self-check table, and `git log -1 --oneline`.
