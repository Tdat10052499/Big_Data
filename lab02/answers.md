# Lab 2 answers: Spark Batch Processing

Student: Hồ Du Tuấn Đạt, 2374802010097, HDFS user `tdat1`.
Every number below is copied from a log in `runs/`; the matching image is in `figures/`. Each image rendered from a log carries that log's SHA-256 in its footer.

## Section 3 (reference run, not an exercise)

Log `runs/sec3_q1_scale.log` (2026-10-16 03:02:52 UTC), image `figures/lab02_sec3_q1_scale.png`.

- Raw rows, January to March: 9,554,778; `curated_q1` rows: 8,480,687; dropped: 1,074,091, which is 11.24% of the 9,554,778 raw Jan-Mar rows; 4 partitions in memory.
- `hdfs dfs -count curated_q1`: 1 directory, 5 files, 187,967,005 bytes.
- All five values match the expected output printed in manual section 3.2.

## Exercise 1: Curate a different window (7 points)

Script `src/ex01_janfeb.py`: the Lab 1 / Section 3 rules unchanged, applied to the two files `yellow_tripdata_2024-01.parquet` and `yellow_tripdata_2024-02.parquet`, with the pickup window [2024-01-01, 2024-03-01). Output: `/user/tdat1/nyc/curated_jan_feb`.
Evidence: `runs/ex01_janfeb.log` (2026-10-16 03:03:29 UTC), `figures/lab02_ex01_janfeb.png`.

| Quantity | Value |
|---|---|
| Raw rows, 2024-01 file | 2,964,624 |
| Raw rows, 2024-02 file | 3,007,526 |
| Raw rows, Jan+Feb (cohort) | 5,972,150 |
| Kept (`curated_jan_feb`) | 5,444,135 |
| Dropped | 528,015 |
| Share dropped | **8.84% of the 5,972,150 raw Jan+Feb rows** |
| Pickup dates kept | 2024-01-01 .. 2024-02-29 |
| `hdfs dfs -count` | 1 directory, 5 files (4 parquet parts + `_SUCCESS`), 120,323,100 bytes |

Consistency checks (arithmetic on the logged numbers): 2,964,624 + 3,007,526 = 5,972,150; 5,972,150 − 5,444,135 = 528,015; 528,015 / 5,972,150 = 8.84%; the four part sizes in `hdfs dfs -ls` add up to 120,323,100 bytes. The January file count equals the Lab 1 raw count (2,964,624).

Comparison, with each cohort named: Lab 1 dropped 8.11% of the 2,964,624 raw January rows; this window drops 8.84% of the 5,972,150 raw Jan+Feb rows; Section 3 dropped 11.24% of the 9,554,778 raw Jan-Mar rows. The share rises as months are added, which is consistent with the manual's statement that later months contain a larger share of failing rows. This is a comparison of aggregates, not a per-month measurement: the drop share of the February file alone was not measured, and the date window also changes between the three runs (rows in the January file with a February pickup are dropped by Lab 1 but kept here), so the three shares are not strictly comparable.

## Exercise 2: Which line does the work (7 points)

Script `src/ex02_lines.py`. Each line of the fragment is timed on its own: 1 warm-up pass of a-e (not scored), then 3 scored passes. Each line also runs in its own Spark job group, so the log records how many Spark jobs that line launched. Settings: `local[4]`, AQE on (default), `spark.sql.shuffle.partitions` = 200 (default). Input: `curated_q1`, 8,480,687 rows.
Evidence: `runs/ex02_lines.log` (2026-10-16 03:23:02 UTC), `figures/lab02_ex02_timings.png`.

| Line | Run 1 (s) | Run 2 (s) | Run 3 (s) | Best (s) | Spark jobs per run |
|---|---|---|---|---|---|
| a `filter` | 0.0118 | 0.0122 | 0.0093 | 0.0093 | 0 / 0 / 0 |
| b `select` | 0.0121 | 0.0218 | 0.0143 | 0.0121 | 0 / 0 / 0 |
| c `groupBy().count()` | 0.0197 | 0.0145 | 0.0173 | 0.0145 | 0 / 0 / 0 |
| d `orderBy` | 0.0161 | 0.0113 | 0.0079 | 0.0079 | 0 / 0 / 0 |
| e `take(5)` | 0.6558 | 0.5635 | 0.4263 | 0.4263 | 2 / 2 / 2 |

**Answer.** Only line e reads data. Lines a-d are transformations: each returns a new DataFrame that only records the step, so Spark launches no job (0 jobs in all three runs) and each line finishes in about 0.01-0.02 s. `take(5)` is an action: it is the first point where a result must be returned, so Spark plans and runs the whole chain a-e, which launched 2 jobs per run. Its best time (0.4263 s) is about 29 times the slowest transformation's best time (0.0145 s, line c).

Limits: the 0.01-0.02 s of lines a-d is driver-side work (building and analysing the logical plan), not data reading; the job count, not the timing, is the direct evidence. Why `take(5)` produced 2 jobs rather than 1 was not measured; a plausible explanation (hypothesis) is that with AQE on, the shuffle for the group-by runs as one job and the ordered `take` on its output runs as a second one. Run-to-run spread for line e is 0.4263-0.6558 s on one laptop.

Result of e (run 3; among `curated_q1` trips with `fare_amount > 10`): PULocationID 132: 400,718; 161: 279,334; 138: 272,507; 186: 213,901; 162: 204,639.

## Exercise 3: Read a plan (7 points)

Script `src/ex03_plan.py`: `curated_q1.groupBy("payment_type").agg(avg("tip_amount"))`, plan taken with `explain(mode="formatted")` before any action. Settings: AQE on (default), `spark.sql.shuffle.partitions` = 200 (default).
Evidence: `runs/ex03_plan.log` (2026-10-16 03:32:56 UTC), `figures/lab02_ex03_plan.png` (Part 1 is the plan as printed by Spark; Part 2 is the same plan with the three answers marked in yellow by the script).

| Question | Answer | Plan line |
|---|---|---|
| Columns the scan reads | **2 of the 22** columns of `curated_q1` | `(1) Scan parquet` → `Output [2]: [payment_type#9L, tip_amount#13]`, `ReadSchema: struct<payment_type:bigint,tip_amount:double>` |
| Filter pushed down | **No** | the `(1) Scan parquet` node has no `PushedFilters` line |
| Exchange steps | **1** | `(3) Exchange` → `hashpartitioning(payment_type#9L, 200), ENSURE_REQUIREMENTS` |

Reading: column pruning applies even with no filter, because only the grouping key and the averaged column are needed. Nothing is pushed down because the query has no `filter`/`where`; the Scan node therefore carries no `PushedFilters` entry (Spark omits the line when the list is empty). The single Exchange sits between the partial `HashAggregate (2)` (`partial_avg`) and the final `HashAggregate (4)` (`avg`), so each of the 200 requested shuffle partitions receives per-partition partial sums and counts, not raw rows. The plan was taken before execution (`isFinalPlan=false`), so AQE may still change the number of partitions actually read after the shuffle; Exercise 6 prints the final plan.

Result printed after the plan (run once): payment_type 1: 4.2010; 2: 0.0002; 3: 0.0064; 4: 0.0157 (average `tip_amount`, over `curated_q1` rows of each type). Only four payment types appear in `curated_q1`. The near-zero average for type 2 is a measured value; the usual explanation (cash tips are not recorded in TLC data) is background knowledge, not something this lab measured.

## Exercise 4: Count the stages (7 points)

Scripts `src/ex04_job.py` (three actions on `curated_q1`, then keeps the UI open) and `src/ex04_capture.py` (reads the counts from the running UI's REST API and screenshots the Jobs tab and one Stage page with headless Chromium on the same machine). Settings: `local[4]`, **AQE off** (so that one action is one job and stage boundaries equal shuffles), `spark.sql.shuffle.partitions` = 200.
Evidence: `runs/ex04_job.log` (03:52:47 UTC), `runs/ex04_ui.log` (03:53:07 UTC), `figures/lab02_ex04_jobs.png`, `figures/lab02_ex04_stage.png`, `figures/lab02_ex04_numbers.png`.

| Question | Answer | Where it shows |
|---|---|---|
| Number of jobs | **6** (jobs 0-5) | Jobs tab: "Completed Jobs: 6"; `ex04_ui.log` job table |
| Stages in the longest job | **3**: job 3 (action 2, wide aggregation), 3.009 s, stages 4, 5, 6, none skipped | Jobs tab row 3: "Stages 3/3", "Tasks 205/205" |
| Tasks in its first stage | **4** (stage 4) | Stage 4 page: "Summary Metrics for 4 Completed Tasks", "Tasks (4)" |
| Shuffles implied | **2** (3 stages − 1) | — |

Reading. The first stage (stage 4) read all 8,480,687 input records (17.5 MiB) in 4 tasks, one per input partition, and wrote 536,217 shuffle records: each task pre-aggregates its own rows before the shuffle (partial count). The job's 205 tasks are consistent with 4 + 200 + 1, i.e. a 4-task scan stage, a 200-task stage after the first shuffle (`spark.sql.shuffle.partitions` = 200) and a 1-task stage after the second shuffle that counts the groups; this breakdown is inferred from the totals, the per-stage task counts of stages 5 and 6 were not captured. The two shuffles are therefore the group-by on (PULocationID, DOLocationID, pickup_hour) and the global `count` of the 230,729 groups.

Other jobs: jobs 0 and 1 are the parquet and CSV reads (schema/file listing, 1 task each); action 1 (`count`) is job 2 with 2 stages; action 3 (join + group by Borough) appears as two jobs, a 1-task job 4 and a 2-stage job 5 with 204 tasks. That the 1-task job is the broadcast of the 265-row zone table is a hypothesis; the plan of action 3 was not printed here (Exercise 7 does that).
Limits: the counts depend on the setting (AQE off, 200 partitions) and on `curated_q1` having 4 input partitions on this machine; with AQE on, each shuffle stage is submitted as its own job (seen in a synthetic-data test before the real run, not in this evidence).
