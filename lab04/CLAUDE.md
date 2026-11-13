# Rules for Claude Code working in lab04/ on the student's machine

Student: Hồ Du Tuấn Đạt, 2374802010097, HDFS user `tdat1`, WSL Ubuntu, Spark 3.5.9, Hadoop 3.4.3, Java 17, Python 3.14.4 (venv `~/bda/venv`), MongoDB started only by `bda-start.sh`.

1. Never invent, edit or round a number. Every number in a log, answer or image comes from a command you actually ran in this session. If a run fails, save the failing output and report it; do not retry silently more than once.
2. Logs: stdout to `runs/<name>.log` with `date -u` on the first line; Spark stderr to `runs/<name>.err`. Never edit a log after it is written.
3. Images: `figures/lab04_*.png`. Terminal evidence is rendered from the real log with `~/bda/venv/bin/python src/log_to_png.py runs/<name>.log figures/lab04_<name>.png` (it stamps the source log name and its SHA-256 on the image). Never draw, retouch or compose an image by hand.
4. Exercises 5-8: the prediction is written by the student. Ask the student for the text, save it verbatim to `runs/exNN_prediction.txt`, commit and push it BEFORE measuring, never change it afterwards. Never write, suggest or hint at a prediction yourself.
5. Timings: 1 warm-up + 3 scored runs; record all three and the best (`[timing]` lines from `src/l0_common.py`). Every percentage names its cohort.
6. Never delete `/user/tdat1/nyc/{raw,ref,curated,curated_q1,agg,stream}`. Never edit `../lab01/` or `../lab02/`. The clean-up commands of manual section 13 are run only when the student asks, after marks are in.
7. Each new WSL session: `bash ~/bda/lab04/start_cluster.sh`, then `source ~/bda/lab04/env.sh` (sets `cd`, venv, `$SS`, `$T`). If start_cluster prints STOP, stop and report.
8. Start MongoDB only with `bda-start.sh` (or `bda-start.sh mongo`), never with `systemctl`.
9. Never commit `out/*.parquet` (copies of HDFS data); they are ignored by `.gitignore`.
10. `sudo` (apt install) only after the student says yes in this session.
11. Git: work only on branch `claude/lab04-storage-b5z7je`. Plain `git push origin claude/lab04-storage-b5z7je`. No force-push, no merge, no rebase, no changes to CI or `.github/`.
12. When done, print the log contents verbatim and the commit hash; do not summarise numbers in your own words.
