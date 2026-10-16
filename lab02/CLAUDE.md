# Rules for Claude Code working in lab02/ on the student's machine

Student: Hồ Du Tuấn Đạt, 2374802010097, HDFS user `tdat1`, WSL Ubuntu, Spark 3.5.9, Hadoop 3.4.3, Java 17, Python 3.14.4 (venv `~/bda/venv`).

1. Never invent, edit or round a number. Every number in a log, answer or image comes from a command you actually ran in this session. If a run fails, save the failing output and report it; do not retry silently more than once.
2. Logs: stdout to `runs/<name>.log` with `date -u` on the first line; Spark stderr to `runs/<name>.err`. Never edit a log after it is written.
3. Images: `figures/lab02_*.png`. Terminal evidence is rendered from the real log with `python3 src/log_to_png.py runs/<name>.log figures/<name>.png` (it stamps the source log name and its SHA-256 on the image). Spark UI evidence is a real browser screenshot of the running UI. Never draw, retouch or compose an image by hand.
4. Exercises 5-8: the prediction is written by the student. Ask the student for the text, save it verbatim to `runs/exNN_prediction.txt`, commit it BEFORE measuring, never change it afterwards. Never write a prediction yourself.
5. Timings: 1 warm-up + 3 scored runs; record all three and the best. Every percentage names its cohort ("of the N raw Jan+Feb rows").
6. Never delete `/user/tdat1/nyc/{raw,ref,curated,curated_q1,agg}`. Never edit `../lab01/`.
7. Each new WSL session: `bash ~/bda/lab02/start_cluster.sh`. If it prints STOP or `Connection refused`, stop and report.
8. `toPandas()` fails on Python 3.14; use `.collect()`.
9. Git: work only on branch `claude/lab02-spark-batch-33099o`. Plain `git push origin claude/lab02-spark-batch-33099o`. No force-push, no merge, no rebase, no changes to CI or `.github/`.
10. When done, print the log contents verbatim and the commit hash; do not summarise numbers in your own words.
