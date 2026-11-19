# Prompt for Claude Code on the laptop's WSL: Exercise 9 (load test)

Paste everything below the line into Claude Code, started with `cd ~/Big_Data && claude` as user `tdat1`.

---

You are working in `~/Big_Data` on the student's laptop (WSL Ubuntu, user `tdat1`). First read `lab04/CLAUDE.md` and follow it strictly. No predictions are needed in this prompt.

Goal: Lab 4 Exercise 9, a load test of Q1 on MongoDB (1, 4, 16 threads) and Spark on Parquet (1, 4 threads), 10 s per level, pushed to branch `claude/lab04-storage-b5z7je`. Close other heavy programs on the laptop first if you can; the test measures this machine.

Step 1. Update and start. If `git status --short` shows changes, or start_cluster prints STOP, stop and show me.
```bash
cd ~/Big_Data && git status --short && git pull origin claude/lab04-storage-b5z7je
bash ~/bda/lab04/start_cluster.sh
```

Step 2. Machine state, then the load test (about 2 minutes; use a 10-minute timeout).
```bash
source ~/bda/lab04/env.sh && { date -u; echo "--- machine"; nproc; lscpu | grep -m1 "Model name"; free -h | head -2; uptime; $SS src/e9_loadtest.py 2>runs/ex09_loadtest.err; echo "--- out/loadtest.csv"; cat out/loadtest.csv; } 2>&1 | tee runs/ex09_loadtest.log
```
If the log has no `wrote out/loadtest.csv` line, show me the last 40 lines of `runs/ex09_loadtest.err` and stop. Run it ONCE; do not re-run to get nicer numbers.

Step 3. Render.
```bash
cd ~/bda/lab04 && ~/bda/venv/bin/python src/log_to_png.py runs/ex09_loadtest.log figures/lab04_ex09_loadtest.png
```

Step 4. Self-check (read only): five rows in the table and in `out/loadtest.csv` (MongoDB 1/4/16, Spark 1/4), each with requests, req/s, median and p95; two "highest measured rate" lines.

Step 5. Commit and push (the CSV is a required deliverable).
```bash
cd ~/Big_Data && git add lab04/runs lab04/figures lab04/out/loadtest.csv && git status --short && git commit -m "Lab 4: Exercise 9 load test" && git push origin claude/lab04-storage-b5z7je
```

Finally print, verbatim: `cat lab04/runs/ex09_loadtest.log`, the self-check, and `git log -1 --oneline`.
