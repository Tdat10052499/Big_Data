# Prompt for Claude Code on the laptop's WSL: Section 5, Exercise 3, Section 10.1-10.2, Exercise 4

Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude` as user `tdat1`.

---

You are working in `~/Big_Data` on the student's laptop (WSL Ubuntu, user `tdat1`). First read `lab04/CLAUDE.md` and follow it strictly: never invent or edit numbers, never write predictions, never delete HDFS data under `/user/tdat1/nyc` except the Lab 4 tables the scripts recreate (`lake/zone_summary`, `lake/zone_hour`), never touch `lab01/` or `lab02/`, never force-push, no `sudo`.

Goal: real evidence for manual Section 5 (Delta Lake), Exercise 3, Section 10.1-10.2 (HDFS access rules, audit fields, trash) and Exercise 4, pushed to branch `claude/lab04-storage-b5z7je`.

Every Bash call may start a fresh shell, so put `source ~/bda/lab04/env.sh &&` in front of every command (it sets `cd ~/bda/lab04`, the venv, `$SS` and `T=/user/tdat1/nyc/lake/zone_summary`).

Step 1. Update and start.
```bash
cd ~/Big_Data && git status --short && git pull origin claude/lab04-storage-b5z7je
bash ~/bda/lab04/start_cluster.sh
```
If `git status --short` shows changes, or start_cluster prints STOP, stop and show me.

Step 2. Section 5.1: create the table and look at commit 0.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/d1_create.py 2>runs/sec5_create.err; echo "--- ls"; hdfs dfs -ls -C $T $T/_delta_log; echo "--- keys of commit 0"; hdfs dfs -cat $T/_delta_log/00000000000000000000.json | jq -c keys; echo "--- add stats"; hdfs dfs -cat $T/_delta_log/00000000000000000000.json | jq 'select(.add) | .add.stats | fromjson | {numRecords, minValues}'; } 2>&1 | tee runs/sec5_create.log
```

Step 3. Section 5.2: one MERGE, then the files and commit 1.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/d2_merge.py 2>runs/sec5_merge.err; echo "--- ls"; hdfs dfs -ls -C $T; echo "--- keys of commit 1"; hdfs dfs -cat $T/_delta_log/00000000000000000001.json | jq -c keys; } 2>&1 | tee runs/sec5_merge.log
```

Step 4. Sections 5.3-5.4: history, time travel, two refusals.
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/d3_history.py 2>runs/sec5_history.err; } | tee runs/sec5_history.log
```
`d3_history.py` tries to append a row with an extra column, which Delta should refuse. Check afterwards that the table still has 2 commits (00..00.json and 00..01.json) in `_delta_log`; include that `ls` in your report.

Step 5. Exercise 3: the student's table `lake/zone_hour` (script already in `src/e3_zone_hour.py`).
```bash
source ~/bda/lab04/env.sh && { date -u; $SS src/e3_zone_hour.py 2>runs/ex03_zone_hour.err; echo "--- files in the table"; hdfs dfs -ls /user/tdat1/nyc/lake/zone_hour; echo "--- actions in commit 1"; hdfs dfs -cat /user/tdat1/nyc/lake/zone_hour/_delta_log/00000000000000000001.json | jq -c keys; } 2>&1 | tee runs/ex03_zone_hour.log
```

Step 6. Section 10.1: an access rule on `lake/zone_summary` that refuses the user `reader`. Some of these commands are EXPECTED to fail; the `exit=` lines record that. If the `-rm` line does NOT print `Permission denied`, stop at once and tell me (it would have deleted the log; `d1_create.py` + `d2_merge.py` rebuild it).
```bash
source ~/bda/lab04/env.sh && { date -u; set -x
hdfs dfs -chmod 750 $T; echo "exit=$?"
HADOOP_USER_NAME=reader hdfs dfs -ls $T; echo "exit=$?"
hdfs dfs -setfacl -m user:reader:r-x $T; echo "exit=$?"
hdfs dfs -getfacl $T; echo "exit=$?"
HADOOP_USER_NAME=reader hdfs dfs -ls -C $T; echo "exit=$?"
HADOOP_USER_NAME=reader hdfs dfs -rm -r $T/_delta_log; echo "exit=$?"
echo "--- undo"
hdfs dfs -setfacl -b $T; echo "exit=$?"
hdfs dfs -chmod 755 $T; echo "exit=$?"
hdfs dfs -getfacl $T
echo "--- authentication"
hdfs getconf -confKey hadoop.security.authentication
set +x; } 2>&1 | tee runs/sec10_acl.log
```

Step 7. Exercise 4: the same rule on `lake/zone_hour`, with the student ID as the reader's name. Same safety rule for the `-rm` line.
```bash
source ~/bda/lab04/env.sh && { date -u; Z=/user/tdat1/nyc/lake/zone_hour; R=2374802010097; set -x
hdfs dfs -ls -d $Z; hdfs dfs -getfacl $Z
hdfs dfs -chmod 750 $Z; echo "exit=$?"
HADOOP_USER_NAME=$R hdfs dfs -ls $Z; echo "exit=$?"
hdfs dfs -setfacl -m user:$R:r-x $Z; echo "exit=$?"
hdfs dfs -getfacl $Z; echo "exit=$?"
HADOOP_USER_NAME=$R hdfs dfs -ls -C $Z; echo "exit=$?"
HADOOP_USER_NAME=$R hdfs dfs -rm -r $Z/_delta_log; echo "exit=$?"
echo "--- undo both changes"
hdfs dfs -setfacl -b $Z; echo "exit=$?"
hdfs dfs -chmod 755 $Z; echo "exit=$?"
hdfs dfs -ls -d $Z; hdfs dfs -getfacl $Z
set +x; } 2>&1 | tee runs/ex04_acl.log
```

Step 8. Section 10.2: what commit 0 records about itself, and the trash setting.
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- commitInfo keys of commit 0"; hdfs dfs -cat $T/_delta_log/00000000000000000000.json | jq -r 'select(.commitInfo) | .commitInfo | keys[]'; echo "--- fs.trash.interval"; hdfs getconf -confKey fs.trash.interval; echo "--- logRetentionDuration set on the table?"; hdfs dfs -cat $T/_delta_log/00000000000000000000.json | jq -c 'select(.metaData) | .metaData.configuration'; } 2>&1 | tee runs/sec10_gov.log
```

Step 9. Render every log to an image.
```bash
cd ~/bda/lab04 && for n in sec5_create sec5_merge sec5_history ex03_zone_hour sec10_acl ex04_acl sec10_gov; do ~/bda/venv/bin/python src/log_to_png.py runs/$n.log figures/lab04_$n.png; done
```

Step 10. Self-check, reading only the logs (change nothing). PASS or FAIL with numbers copied from the logs:
- `sec5_create.log`: rows 258, version 0; commit 0 has the four actions commitInfo, metaData, protocol, add; numRecords 258;
- `sec5_merge.log`: version 1, MERGE; the six metrics are printed; rows in the table now = 260; `ls` shows two Parquet files; commit 1 keys are commitInfo, add, remove;
- `sec5_history.log`: two versions in the history; version 0 and latest row counts and zone 132 trips printed; part 3 and part 4 each print a `refused:` line;
- `ex03_zone_hour.log`: version 0 has 5,422 rows (manual Table 1); the latest version has no hour-3 rows; the DELETE operationMetrics are printed;
- `sec10_acl.log` and `ex04_acl.log`: the first reader `-ls` and the `-rm` print `Permission denied`, the ACL `-ls -C` succeeds (exit=0), `getfacl` shows the named user and a mask, the final `getfacl` has no named user;
- `sec10_gov.log`: no user field among the commitInfo keys; trash interval printed.

Step 11. Commit and push the evidence and the Exercise 3 script.
```bash
cd ~/Big_Data && git add lab04/runs lab04/figures lab04/src/e3_zone_hour.py && git status --short && git commit -m "Lab 4: Section 5, Exercise 3, Section 10.1-10.2, Exercise 4 evidence" && git push origin claude/lab04-storage-b5z7je
```

Finally print, verbatim: `cat` of the seven logs from Step 9, the `_delta_log` listing from Step 4, the self-check table, and `git log -1 --oneline`.
