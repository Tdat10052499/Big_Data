# Prompt for Claude Code on the NEW laptop's WSL: rebuild the Lab 1-2 data, then run P1

Before pasting: finish the manual steps in `lab04/setup_laptop/README.md` (WSL, the two setup scripts, Claude Code, `gh auth login`).
Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude` in a NEW Ubuntu terminal.

---

You are working in `~/Big_Data` on the student's new laptop (WSL Ubuntu). This laptop has a fresh HDFS. The Lab 1 and Lab 2 datasets do not exist on it yet and must be rebuilt from the original NYC TLC files with the student's own Lab 1 and Lab 2 scripts, exactly as on the old machine. First read `lab04/CLAUDE.md` and follow it strictly: never invent or edit numbers, never write predictions, never edit `lab01/` or `lab02/` (running their scripts is fine; logs go to `lab04/runs/`), never force-push, no `sudo`.

Step 1. Branch and environment check. If `git status --short` shows uncommitted changes, stop and show them to me.
```bash
cd ~/Big_Data && git status --short && git checkout claude/lab04-storage-b5z7je && git pull origin claude/lab04-storage-b5z7je
bash lab04/setup_lab04.sh
bash ~/bda/lab04/start_cluster.sh
```
If start_cluster prints STOP, stop and show me the output.

Step 2. Machine evidence (the new laptop).
```bash
cd ~/bda/lab04 && { date -u; echo "host: $(hostname)  user: $(whoami)"; grep PRETTY /etc/os-release; nproc; free -h | head -2; java -version 2>&1 | head -1; hadoop version | head -1; spark-submit --version 2>&1 | grep -m1 version; ~/bda/venv/bin/python --version; bda-status.sh; } 2>&1 | tee runs/rebuild_machine.log
```

Step 3. Download the four source files and check them against the SHA-256 that Lab 1 recorded on the old machine.
```bash
cd ~/bda/data/raw && B=https://d37ci6vzurychx.cloudfront.net
for m in 01 02 03; do [ -f yellow_tripdata_2024-$m.parquet ] || wget -q $B/trip-data/yellow_tripdata_2024-$m.parquet; done
[ -f taxi_zone_lookup.csv ] || wget -q $B/misc/taxi_zone_lookup.csv
cd ~/bda/lab04 && { date -u; ls -l ~/bda/data/raw; (cd ~/bda/data/raw && sha256sum -c ~/Big_Data/lab01/runs/source_checksums.txt); } 2>&1 | tee runs/rebuild_download.log
```
If any line does not say `OK`, STOP and show me the log. Do not continue with a different file.

Step 4. Put them in HDFS where Labs 1-2 expect them.
```bash
N=/user/tdat1/nyc && hdfs dfs -mkdir -p $N/raw $N/ref && hdfs dfs -put -f ~/bda/data/raw/yellow_tripdata_2024-0*.parquet $N/raw/ && hdfs dfs -put -f ~/bda/data/raw/taxi_zone_lookup.csv $N/ref/
cd ~/bda/lab04 && { date -u; hdfs dfs -ls $N/raw $N/ref; } 2>&1 | tee runs/rebuild_hdfs_put.log
```

Step 5. Rebuild with the student's own scripts (read from the repo, not changed).
```bash
cd ~/bda/lab04 && source ~/bda/lab04/env.sh
{ date -u; spark-submit ~/Big_Data/lab01/src/p4_curate.py 2>runs/rebuild_p4_curate.err; hdfs dfs -count /user/tdat1/nyc/curated; } | tee runs/rebuild_p4_curate.log
{ date -u; spark-submit ~/Big_Data/lab02/src/q1_scale.py 2>runs/rebuild_q1_scale.err; hdfs dfs -count /user/tdat1/nyc/curated_q1; } | tee runs/rebuild_q1_scale.log
{ date -u; spark-submit ~/Big_Data/lab02/src/q5_write.py 2>runs/rebuild_q5_write.err; hdfs dfs -ls -C /user/tdat1/nyc/agg; hdfs dfs -count /user/tdat1/nyc/agg/zone_summary /user/tdat1/nyc/agg/zone_hour; } | tee runs/rebuild_q5_write.log
```
If a log is missing its printed numbers, show me the last 40 lines of its `.err` and stop.

Step 6. Row counts of what Lab 4 reads (manual Table 1).
```bash
cd ~/bda/lab04 && source ~/bda/lab04/env.sh && cat > /tmp/rebuild_counts.py <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.master("local[4]").appName("rebuild-counts").getOrCreate()
s.sparkContext.setLogLevel("ERROR")
N = "hdfs://localhost:9000/user/tdat1/nyc"
for p in ["curated", "curated_q1", "agg/zone_summary", "agg/zone_hour"]:
    print(f"  {p:22}{s.read.parquet(f'{N}/{p}').count():>12,}")
print(f"  {'ref/taxi_zone_lookup':22}{s.read.option('header', True).csv(f'{N}/ref/taxi_zone_lookup.csv').count():>12,}")
s.stop()
PY
{ date -u; spark-submit /tmp/rebuild_counts.py 2>runs/rebuild_counts.err; } | tee runs/rebuild_counts.log
```

Step 7. Self-check, reading only the logs. PASS or FAIL with the numbers copied from the logs, against the old machine's values:
- `rebuild_p4_curate.log`: raw 2,964,624 and kept 2,724,143 (Lab 1);
- `rebuild_q1_scale.log`: raw 9,554,778 and kept 8,480,687 (Lab 2 run record);
- `rebuild_counts.log`: curated 2,724,143; curated_q1 8,480,687; zone_summary 258; zone_hour 5,422; zone lookup 265 (manual Table 1).
A FAIL is not a reason to change anything: report it and stop.

Step 8. Render and push.
```bash
cd ~/bda/lab04 && for n in rebuild_machine rebuild_download rebuild_hdfs_put rebuild_p4_curate rebuild_q1_scale rebuild_q5_write rebuild_counts; do ~/bda/venv/bin/python src/log_to_png.py runs/$n.log figures/lab04_$n.png; done
cd ~/Big_Data && git add lab04/runs lab04/figures && git commit -m "Lab 4: rebuild Lab 1-2 data on the new laptop" && git push origin claude/lab04-storage-b5z7je
```

Step 9. Only if every check in Step 7 is PASS: open `lab04/prompts/p1_env_sec4_ex01_ex02.md` and carry out its Steps 2-13 (its Step 1 is already done). Lab 3's `stream/ck/restart` will be missing on this laptop; that is expected, note it and continue as that prompt says.

Finally print, verbatim: `cat lab04/runs/rebuild_*.log`, the Step 7 table, then everything P1's last line asks for, and `git log -3 --oneline`.
