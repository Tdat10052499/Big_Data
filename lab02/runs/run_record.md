# Run record: Lab 2 (Spark Batch Processing)

Student: Hồ Du Tuấn Đạt - 2374802010097
Linux account: tdat1 (machine DESKTOP-ANNRGGL, from the Spark warnings in `runs/*.err`)

Conventions: every number is copied from a log in `runs/`. Timings are 1 warm-up run (not scored) + 3 scored runs; all three and the best are recorded. Every percentage names its cohort. Times are UTC.

## 1. Environment (`runs/ex00_env.log`, 2026-10-16 03:01:15)

| Item | Value |
|---|---|
| Spark | 3.5.9 (`spark-submit --version`) |
| Python | 3.14.4 |
| Hadoop / Java | 3.4.3 / OpenJDK 17 (from Lab 1 run record; not re-printed in Lab 2) |
| Services UP | HDFS NameNode, HDFS DataNode, YARN ResourceManager, YARN NodeManager, MapReduce JobHistoryServer, sshd |
| Services DOWN | Spark History Server, Kafka broker, MongoDB (not needed for Ex1-3) |
| HDFS | 1 live DataNode, safe mode OFF |
| `curated` (Lab 1) | 1 dir, 4 files, 60,289,030 bytes |
| `curated_q1` | 1 dir, 5 files, 187,967,005 bytes |
| Spark master | `local[4]` (in every script) |

## 2. Changes to the lecturer's code
- `/user/thaianh/` replaced by `/user/tdat1/` in q1-q6 (no `thaianh` left).
- Added `common.py`, `ex01_janfeb.py`, `ex02_lines.py`, `ex03_plan.py`, `ex04_job.py`, `ex04_capture.py`, `ex05_partitions.py`, `log_to_png.py`.

## 3. Runs

| When (UTC) | What | Log | Result |
|---|---|---|---|
| 03:02:52 | Section 3, `q1_scale.py` (re-run; rewrites `curated_q1` with the same rules) | `sec3_q1_scale.log` | raw 9,554,778; kept 8,480,687; dropped 1,074,091 = 11.24% of the 9,554,778 raw Jan-Mar rows; 4 partitions; count 1 / 5 / 187,967,005 bytes (same bytes as before the re-run) |
| 03:03:29 | Ex1, `ex01_janfeb.py` | `ex01_janfeb.log` | raw 5,972,150 (01: 2,964,624; 02: 3,007,526); kept 5,444,135; dropped 528,015 = 8.84% of the 5,972,150 raw Jan+Feb rows; 5 partitions in memory; count 1 / 5 / 120,323,100 bytes |
| 03:23:02 | Ex2, `ex02_lines.py` (AQE on, 200 shuffle partitions) | `ex02_lines.log` | best of 3 (s): a 0.0093, b 0.0121, c 0.0145, d 0.0079, e 0.4263; Spark jobs per run: a-d 0, e 2; all runs in `answers.md` |
| 03:32:56 | Ex3, `ex03_plan.py` (AQE on, 200 shuffle partitions; plan before execution) | `ex03_plan.log` | scan reads 2 of 22 columns; no PushedFilters; 1 Exchange `hashpartitioning(payment_type, 200)` |
| 03:52:47 | Ex4, `ex04_job.py` + `ex04_capture.py` (AQE off, 200 shuffle partitions) | `ex04_job.log`, `ex04_ui.log` | 6 jobs; longest job 3 (3.009 s): 3 stages, 205 tasks; first stage 4: 4 tasks, 8,480,687 input records, 536,217 shuffle records; 2 shuffles; wide aggregation 230,729 groups; screenshots by headless Chromium on the student machine |

Note on Ex1: the output has 4 part files but the read-back DataFrame has 5 partitions in memory. This is an inference, not a measurement: when reading, Spark splits files larger than its split size and packs small splits together, so the partition count need not equal the file count.

## 4. Predictions (Ex5-8)
Not yet written.
