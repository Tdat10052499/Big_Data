# Run record: Lab 4 (Storage and Serving Architecture)

Student: Hồ Du Tuấn Đạt - 2374802010097
Linux / HDFS account: tdat1, machine LAPTOP-21KJQDU4 (`rebuild_machine.log`)

Conventions: every number is copied from a log in `runs/`; each log starts with `date -u` and has an image `figures/lab04_<log>.png` rendered from it by `src/log_to_png.py` (the image footer carries the log's SHA-256). Timings are 1 warm-up + 3 scored runs; all three and the best are in the `[timing]` lines. Every percentage names its cohort. All times are UTC on 2026-11-19.

## 1. Software versions (`ex00_env.log`, `rebuild_machine.log`)

| Item | Value |
|---|---|
| OS | Ubuntu 24.04.5 LTS on WSL2, 16 cores (AMD Ryzen AI 7 350), 15 GiB RAM |
| Java | OpenJDK 17.0.21.1 |
| Hadoop | 3.4.3 (pseudo-distributed, replication 1, `hadoop.security.authentication` = simple) |
| Spark | 3.5.9, master `local[4]`; packages `io.delta:delta-spark_2.12:3.2.1`, `org.mongodb.spark:mongo-spark-connector_2.12:10.4.0` |
| Python (venv `~/bda/venv`) | 3.12.3; pyarrow 26.0.1; pymongo 4.20.1 |
| MongoDB | mongod 8.0.38, mongosh 2.14.3, data in `/data/db`, bound to 127.0.0.1, access control off |
| jq | 1.7 |

## 2. Deviations from the manual
1. New machine. Labs 1-2 ran on DESKTOP-ANNRGGL (Ubuntu 26.04, Python 3.14.4). Lab 4 runs on a new laptop set up from scratch with `setup_laptop/` (Ubuntu 24.04, the manual's version; Python 3.12.3). The Hadoop and Spark archives were checked against their published SHA-512.
2. `bda-start.sh`, `bda-status.sh`, `bda-stop.sh` are my own (`setup_laptop/bin/`), because the lecturer's are not on this machine. They start HDFS with `hdfs --daemon` (no YARN, which Lab 4 does not use) and MongoDB with `mongod --fork --dbpath /data/db --bind_ip 127.0.0.1`.
3. Lab 1-2 inputs were rebuilt on this laptop from the TLC files with the unchanged Lab 1 and Lab 2 scripts (Section 4 of this record).
4. Paths: every `/user/thaianh/` is `/user/tdat1/`.
5. Code changes to the lecturer's scripts: `best_of_three` in `l0_common.py` also prints the warm-up and all three runs; `f1_formats.py` and `s1_compare.py` pass a label to it; `f1_formats.py` gained the two Exercise 5 queries. New scripts: `e3_zone_hour.py`, `e6_merge.py`, `e7_index.js`, `e8_append.py`, `e9_loadtest.py`, `log_to_png.py`.
6. Exercise 4 reader name: `2374802010097`.
7. Section 10.3 queries were run with `mongosh --eval` instead of at the interactive prompt, so they could be logged; the startup warnings were read with `getLog: "startupWarnings"`.
8. Lab 3 was not run on this machine, so `stream/ck/restart` is missing (`sec2_inputs.log`, `sec7_logs.log`).

## 3. Seeds
Random zones: seed 42 (`m2_lookup.py`, `s1_compare.py`); Exercise 9 uses 42 + thread number, warm-up 1000 + thread number.

## 4. Rebuild of the inputs (Labs 1-2 scripts, unchanged)

| Time | What | Log | Result |
|---|---|---|---|
| 18:24:19 | download + SHA-256 against `lab01/runs/source_checksums.txt` | `rebuild_download.log` | 4 of 4 `OK` |
| 18:24:31 | `hdfs dfs -put` to `nyc/raw`, `nyc/ref` | `rebuild_hdfs_put.log` | 3 trip files, 1 lookup |
| 18:24:37 | `lab01/src/p4_curate.py` | `rebuild_p4_curate.log` | raw 2,964,624; kept 2,724,143; dropped 240,481 = 8.11% of the 2,964,624 raw January rows |
| 18:25:11 | `lab02/src/q1_scale.py` | `rebuild_q1_scale.log` | raw 9,554,778; kept 8,480,687; dropped 1,074,091 = 11.24% of the 9,554,778 raw Jan-Mar rows |
| 18:25:48 | `lab02/src/q5_write.py` | `rebuild_q5_write.log` | zone_summary 258 rows, zone_hour 5,422 rows, 1 file each |
| 18:27:19 | row counts | `rebuild_counts.log` | curated 2,724,143; curated_q1 8,480,687; zone_summary 258; zone_hour 5,422; zone lookup 265 (all equal to manual Table 1 and to the old machine) |

## 5. Runs and outputs, by manual section

| Time | Section / exercise | Script, options | Input (rows) | Log | Output kept |
|---|---|---|---|---|---|
| 18:28:28 | 2.3, 3 | `hdfs dfs -ls/-count/-du` | - | `sec2_inputs.log` | zone_summary 7.4 K, curated 57.5 M |
| 18:28:42 | 4.1 | `f1_formats.py --write` | curated (2,724,143) | `sec4_write.log` | CSV 356.5 M, Parquet 57.5 M |
| 18:29:25 | 4.2 | `f1_formats.py --plan` | fmt/2024-01 (2,724,143) | `sec4_plan.log` | ratio 6.2x; bytes CSV/Parquet: count 356.5 MB/25 KB, sum 356.5 MB/3.3 MB, all columns 356.5 MB/57.4 MB; both scans `ReadSchema: struct<fare_amount:double>` |
| 18:30:26 | 4.3 | `f2_footer.py out/sample.parquet` | largest of 3 files (1,010,014) | `sec4_footer.log` | 1 row group; pickup_date 2024-01-01..2024-01-13 in 3 KB |
| 18:30:35, 18:31:18 | Ex1 | `f1_formats.py --write --month 2024-02`, then measure | curated_q1 Feb (2,720,001) | `ex01_write.log`, `ex01_measure.log` | see `answers.md` Ex1 |
| 18:32:20 | Ex2 | `f2_footer.py out/zone_hour.parquet PULocationID pickup_hour zone_hour_avg` | agg/zone_hour (5,422) | `ex02_footer.log` | see `answers.md` Ex2 |
| 18:38:04 | 5.1 | `d1_create.py` + jq | agg/zone_summary (258) | `sec5_create.log` | version 0, actions commitInfo/metaData/protocol/add, numRecords 258 |
| 18:38:54 | 5.2 | `d2_merge.py` | 5 corrections | `sec5_merge.log` | source 5, updated 3, inserted 2, copied 255, files -1/+1; 260 rows |
| 18:39:43 | 5.3-5.4 | `d3_history.py` | lake/zone_summary | `sec5_history.log` | v0 258 rows, zone 132 404,725 trips; latest 260, 404,850; refused: `[_LEGACY_ERROR_TEMP_DELTA_0007] A schema mismatch detected when writing to the Delta table`; refused: `requirement failed: Are you sure you would like to vacuum files with such a low retention period?`; userName NULL |
| 18:40:57 | Ex3 | `e3_zone_hour.py` | agg/zone_hour (5,422) | `ex03_zone_hour.log` | see `answers.md` Ex3 |
| 18:41:50 | 10.1 | chmod/setfacl as `reader` | lake/zone_summary | `sec10_acl.log` | refused READ_EXECUTE, allowed after ACL, refused WRITE on rm; restored; auth `simple` |
| 18:42:17 | Ex4 | same, reader `2374802010097` | lake/zone_hour | `ex04_acl.log` | see `answers.md` Ex4 |
| 18:42:44 | 10.2 | jq commitInfo keys, getconf | lake/zone_summary | `sec10_gov.log` | no user key; `fs.trash.interval` 0; table configuration `{}` |
| 18:49:02 | 6 | `d4_readers.py parquet` | curated (2,724,143) / short (1,596,003) | `sec6_parquet.log` | 149 reads, 0 whole; 146 x 0 rows, 3 x FileNotFoundException; 18 s |
| 18:49:52 | 6 | `d4_readers.py delta` | same | `sec6_delta.log` | 47 reads, 47 whole; 35 s |
| 18:51:04 | 7 | `hdfs dfs -ls -C` | stream/ck/restart, _delta_log | `sec7_logs.log` | checkpoint missing; log has commits 0 and 1 |
| 18:51:11 | 8.1-8.2 | `m1_load.py`, findOne | agg/zone_summary (258) | `sec8_load.log` | 258 in, 258 out; columns 4 -> 5 (`_id`); zone_profile 258 |
| 18:51:49 | 8.3 | `m2_lookup.py` | bda.zone_summary (258) | `sec8_lookup.log` | COLLSCAN docs 258 -> FETCH > IXSCAN keys 1 docs 1; median 0.491 -> 0.425 ms, p95 0.708 -> 0.643 ms (1,000 lookups) |
| 18:51:54 | 9.1 | `m3_trips.py` | curated (2,724,143) | `sec9_trips.log` | load 17.8 s; Parquet 57.5 MB; MongoDB 216.5 MB on disk (3.8x), 1,319.8 MB unpacked (23.0x), indexes 26.5 MB |
| 18:52:40 | 9.2 | `s1_compare.py` | as above | `sec9_compare.log` | Q1 median/p95: MongoDB 0.4/0.7 ms (1,000), Spark Parquet 131.3/157.6 ms (20), Spark Delta 315.3/518.8 ms (20); Q2 best of 3: Spark Parquet 0.19 s (runs 0.208/0.191/0.226), MongoDB 1.04 s (runs 1.038/1.054/1.053); agree within 2.0e-13 dollars |
| 18:53:35 | 10.3 | mongosh `--eval` | bda.zone_summary (258) | `sec10_reveal.log` | warning "Access control is not enabled for the database"; 19 zones < 10 trips; 6 zones with 1 trip (44, 99, 105, 115, 156, 176) |
| 19:48:55 | Ex5 | `f1_formats.py` (5 queries) | fmt/2024-01 (2,724,143) | `ex05_measure.log` | see `answers.md` Ex5 |
| 19:50:16, 19:58:23, 19:59:11 | Ex6 | `d1_create.py`, `e6_merge.py`, then `d1_create.py` + `d2_merge.py` to restore | 7 corrections | `ex06_facts.log`, `ex06_merge.log`, `ex06_restore.log` | see `answers.md` Ex6; restored to 260 rows |
| 20:15:03 | Ex7 | `mongosh bda --quiet src/e7_index.js` | bda.trips_jan (2,724,143) | `ex07_index.log` | see `answers.md` Ex7 |
| 20:24:17 | Ex8 | `e8_append.py parquet` | - | `ex08_parquet_fail1.log` | failed: HDFS `Connection refused` at start, nothing written; run once more |
| 20:25:40 .. 20:28:37 | Ex8 | `e8_append.py parquet`, `delta`, `parquet 64`, `delta 64` | curated + 6 x short | `ex08_*.log` | see `answers.md` Ex8 |
| 20:34:23 | Ex9 | `e9_loadtest.py` | bda.zone_summary, agg/zone_summary (258) | `ex09_loadtest.log`, `out/loadtest.csv` | see `answers.md` Ex9 |

Decision recorded (Section 5.4): no `mergeSchema` write was made; the `surge` column was not added.

## 6. Predictions (Exercises 5-8), each committed before its measurement

| Exercise | File | Commit (UTC) | Measurement starts (UTC) | Content |
|---|---|---|---|---|
| 5 | `ex05_prediction.txt` | 53883a4, 19:48:47 | 19:48:55 | an AI worked example for a different query, pasted verbatim (see Section 7) |
| 6 | `ex06_prediction.txt` | 93d3f09, 19:58:13 | 19:58:23 | template with "No prediction written." in every field |
| 7 | `ex07_prediction.txt` | f505589, 20:14:56 | 20:15:03 | template with "No prediction written." in every field |
| 8 | `ex08_prediction.txt` | b471166, 20:24:10 | 20:24:17 (failed), 20:25:40 | template with "No prediction written." in every field; no second prediction for 64 files |

## 7. Statement of AI assistance
I used Claude (Anthropic), as a cloud assistant and as Claude Code on my laptop.
- Produced by the AI: the laptop setup scripts (`setup_laptop/`), the step-by-step prompts in `prompts/`, the new scripts (`e3_zone_hour.py`, `e6_merge.py`, `e7_index.js`, `e8_append.py`, `e9_loadtest.py`, `log_to_png.py`) and the small changes to `l0_common.py`, `f1_formats.py`, `s1_compare.py`; Claude Code on my laptop ran the commands and wrote the logs and images; the drafts of `answers.md`, this run record and `out/ADR-001.md`, written from the logs.
- Predictions: the AI did not write the Exercise 6-8 predictions; I recorded none. For Exercise 5 the recorded text is a worked example the AI wrote for the Section 4 query `sum of fare_amount`, which I pasted unchanged; its numbers are therefore not my own working for the Exercise 5 queries. The AI also gave me fill-in templates, which appear in the Exercise 6-8 files.
- How I checked it: every number in `answers.md`, this record and the ADR was compared against the log it cites; the rebuilt inputs were checked against the Lab 1 SHA-256 values and against the row counts of the old machine and manual Table 1; each prediction's commit time was checked to be before its measurement; each image carries the SHA-256 of its log.
