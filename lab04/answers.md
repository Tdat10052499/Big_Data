# Lab 4 answers: Storage and Serving Architecture

Student: Hồ Du Tuấn Đạt, 2374802010097 (HDFS user `tdat1`).
Machine: laptop LAPTOP-21KJQDU4, WSL Ubuntu 24.04.5, 16 cores, 15 GiB RAM (`runs/rebuild_machine.log`). Spark `local[4]`.
Every number below is copied from a log in `runs/`; the image of each log is `figures/lab04_<log name>.png`.
Timings are one warm-up run plus three scored runs; the table value is the best of the three, and all three are in the `[timing]` lines of the log.

## Before the exercises: inputs rebuilt on this laptop

The Lab 1-2 datasets were rebuilt on this laptop from the original TLC files, with the Lab 1 and Lab 2 scripts unchanged.
All four source files matched the SHA-256 values recorded in Lab 1 (`runs/rebuild_download.log`: four `OK`).

| Dataset (HDFS, `/user/tdat1/nyc/...`) | Rows on this laptop | Manual Table 1 | Log |
|---|---|---|---|
| `curated` (January) | 2,724,143 | 2,724,143 | `rebuild_counts.log` |
| `curated_q1` (Jan-Mar) | 8,480,687 | 8,480,687 | `rebuild_counts.log` |
| `agg/zone_summary` | 258 | 258 | `rebuild_counts.log` |
| `agg/zone_hour` | 5,422 | 5,422 | `rebuild_counts.log` |
| `ref/taxi_zone_lookup.csv` | 265 | 265 | `rebuild_counts.log` |

`stream/ck/restart` (Lab 3) does not exist on this laptop (`runs/sec2_inputs.log`); only Section 7 reads it.

## Exercise 1: A second month (7 points)

Evidence: `runs/ex01_write.log`, `runs/ex01_measure.log` (February), compared with `runs/sec4_write.log`, `runs/sec4_plan.log` (January).
Store: HDFS. Dataset: `fmt/2024-02/{csv,parquet}`, written from `curated_q1` filtered to February.

| | January 2024 (`curated`) | February 2024 (`curated_q1`, Feb only) |
|---|---|---|
| Rows | 2,724,143 | 2,720,001 |
| Stored as CSV | 356.5 MB | 356.0 MB |
| Stored as Parquet | 57.5 MB | 57.4 MB |
| Ratio CSV / Parquet | 6.2x | 6.2x |

Bytes read from HDFS (Spark task input metrics), February:

| Query | CSV bytes | Parquet bytes | CSV best time | Parquet best time |
|---|---|---|---|---|
| count the rows | 356.0 MB | 29 KB | 0.37 s | 0.16 s |
| sum one column (`fare_amount`) | 356.0 MB | 3.5 MB | 1.00 s | 0.17 s |
| count every column | 356.0 MB | 57.3 MB | 2.55 s | 0.72 s |

(January, same queries: CSV 356.5 MB each time; Parquet 25 KB, 3.3 MB, 57.4 MB.)

Comparison. The ratio did not move from January's 6.2x at the one decimal the script prints. The pattern is the same as January: every CSV query read the whole CSV copy, while Parquet read a footer-sized amount to count rows, one column's chunks to sum `fare_amount`, and almost the whole file only when every column was needed.

One property that could move the ratio: how repetitive the values are. Parquet's dictionary and run-length encoding shrink columns with few distinct values or long runs (for example `pickup_date`, 13 distinct values in 3 KB in the January footer, `runs/sec4_footer.log`), while CSV writes every value out in full. A month with more distinct values per column (more distinct fares or timestamps, fewer nulls, or rows in a less sorted order) would compress less in Parquet and lower the ratio. This is a hypothesis; the lab did not measure it.

Limit. The sizes are rounded to 0.1 MB by `hdfs dfs -du -h`, so a change in the ratio smaller than about 0.05x would not show.

## Exercise 2: Read a footer you did not write (7 points)

Evidence: `runs/ex02_footer.log`. Store: one Parquet file on HDFS, `agg/zone_hour` (Lab 2), copied to `out/zone_hour.parquet`.

| Item | Value |
|---|---|
| Rows | 5,422 |
| Row groups | 1 |
| Columns | 3 |
| `PULocationID` min / max | 1 / 265 |
| `pickup_hour` min / max | 0 / 23 |
| `zone_hour_avg` min / max | 0.0 / 494.0 |

These statistics would not let a reader skip this file when it looks for zone 132, because 132 lies between the file's minimum `PULocationID` (1) and maximum (265), and the file holds a single row group, so there is no smaller range that could rule part of it out.

## Exercise 3: A Delta table of my own (7 points)

Evidence: `src/e3_zone_hour.py` (copied from `d1_create.py`, changed to build `lake/zone_hour` from `agg/zone_hour` and then delete hour 3), `runs/ex03_zone_hour.log`.
Store: Delta Lake on HDFS, `/user/tdat1/nyc/lake/zone_hour`. Dataset: Lab 2 `agg/zone_hour` (5,422 rows).

History (newest first):

| Version | Operation | Parameters |
|---|---|---|
| 1 | DELETE | `predicate -> ["(pickup_hour#556 = 3)"]` |
| 0 | WRITE | `mode -> ErrorIfExists, partitionBy -> []` |

Row count at each version:

| Version | Rows | Rows with pickup hour 3 |
|---|---|---|
| 0 | 5,422 | 189 |
| 1 | 5,233 | 0 |

operationMetrics of the DELETE (version 1), selected: `numDeletedRows` 189, `numCopiedRows` 5233, `numRemovedFiles` 1, `numAddedFiles` 1, `numRemovedBytes` 47886, `numAddedBytes` 46505, `numDeletionVectorsAdded` 0, `executionTimeMs` 2523 (all 13 metrics are in the log).

Why 5,233 rows were copied. The table at version 0 is one Parquet file of 5,422 rows (47,886 bytes, the same file size as `agg/zone_hour`, `runs/sec2_inputs.log`). Delta does not edit a Parquet file in place, and no deletion vectors were used (`numDeletionVectorsAdded` 0). To remove the 189 hour-3 rows it read that one file, wrote a new file (46,505 bytes) holding the other 5,422 - 189 = 5,233 rows unchanged, and commit 1 records a `remove` of the old file and an `add` of the new one (`runs/ex03_zone_hour.log`, actions in commit 1). So the cost is set by the size of the file the rows live in: deleting 189 rows meant rewriting all 5,233 others. Both files are still on HDFS (the listing shows two Parquet files), because version 0 still needs the old one until a VACUUM.

## Exercise 4: A rule that refuses someone (7 points)

Evidence: `runs/ex04_acl.log`, in this order. Store: HDFS. Path: `/user/tdat1/nyc/lake/zone_hour` (from Exercise 3). Reader name: `2374802010097`.

1. Before: `drwxr-xr-x tdat1 supergroup`, `getfacl` shows only `user::rwx group::r-x other::r-x`.
2. Lock: `hdfs dfs -chmod 750` (exit=0).
3. Refusal: `ls: Permission denied: user=2374802010097, access=READ_EXECUTE, inode="/user/tdat1/nyc/lake/zone_hour":tdat1:supergroup:drwxr-x---` (exit=1).
4. Grant: `hdfs dfs -setfacl -m user:2374802010097:r-x` (exit=0); `getfacl` shows `user:2374802010097:r-x`, `mask::r-x`, `other::---`.
5. Successful listing as the same reader: `_delta_log` and the two Parquet files (exit=0).
6. Delete refused: `rm: Permission denied: user=2374802010097, access=WRITE, inode="/user/tdat1/nyc/lake/zone_hour":tdat1:supergroup:drwxr-x---` (exit=1). Deleting `_delta_log` needs WRITE on its parent directory, and the ACL entry grants only r-x.
7. Undo: `setfacl -b` (exit=0) and `chmod 755` (exit=0); afterwards `drwxr-xr-x` and `getfacl` shows no named user, the same as step 1.

What the rule does not protect against: `hadoop.security.authentication` is `simple` (`runs/sec10_acl.log`), so HDFS believes whatever name the client sends. I became `2374802010097` just by setting `HADOOP_USER_NAME`; anyone on this machine can equally claim `tdat1`, which owns the NameNode and is therefore the HDFS superuser, to whom no permission check applies. The rule stops mistakes, not attackers; proving identity would need Kerberos, which this course does not set up.

## Exercise 5: Predict the bytes before you read them (10 points)

Evidence: prediction `runs/ex05_prediction.txt` (committed 53883a4, 19:48:47 UTC), facts `runs/ex05_facts.log`, measurement `runs/ex05_measure.log` (19:48:55 UTC).
Store: HDFS. Dataset: `fmt/2024-01/{csv,parquet}`, 2,724,143 January rows.

What was recorded as the prediction. The prediction file holds, verbatim, a worked example that an AI assistant gave me for the Section 4 query `sum of fare_amount` (its first line says so). I submitted it unchanged because I was out of time. So the recorded prediction is: Parquet reads about 3.4 MB (footer column size x 2,724,143 / 1,010,014 rows), CSV reads about 356.5 MB (the whole CSV copy), and it was not worked out separately for the two Exercise 5 queries. This is declared in the AI assistance statement of `runs/run_record.md`.

Measurement (bytes read from HDFS, Spark task input metrics; best of three after a warm-up):

| Query | CSV bytes | Parquet bytes | CSV best | Parquet best |
|---|---|---|---|---|
| sum tip_amount | 356.5 MB | 3.9 MB | 1.04 s | 0.10 s |
| avg fare_amount by payment_type | 356.5 MB | 3.9 MB | 1.06 s | 0.14 s |

Comparison with the recorded prediction:

| Query, store | Recorded prediction | Measured | Off by |
|---|---|---|---|
| sum tip_amount, Parquet | about 3.4 MB | 3.9 MB | x1.15 |
| sum tip_amount, CSV | about 356.5 MB | 356.5 MB | x1.0 |
| avg fare by payment, Parquet | about 3.4 MB | 3.9 MB | x1.15 |
| avg fare by payment, CSV | about 356.5 MB | 356.5 MB | x1.0 |

No prediction was off by more than a factor of two. Explanation of the gap, worked out after the measurement from the footer in `runs/ex05_facts.log`: the recorded number used the `fare_amount` column (1,285 KB in the largest file). The first query reads `tip_amount` instead, which is larger in the footer (1,438 KB); scaled by rows, 1,438 KB x 2,724,143 / 1,010,014 = about 3.79 MB. The second query reads two columns, `payment_type` (222 KB) and `fare_amount` (1,285 KB), so about (222 + 1,285) KB x 2,724,143 / 1,010,014 = about 3.97 MB. Both are close to the measured 3.9 MB, so the rule "Parquet reads only the column chunks the query needs" holds; the recorded number was low because it counted the wrong column, not because the rule failed. The CSV reader read the full 356.5 MB for both, as it did for every query in Section 4.

## Exercise 6: Predict a MERGE (10 points)

Evidence: prediction `runs/ex06_prediction.txt` (committed 93d3f09, 19:58:13 UTC), facts `runs/ex06_facts.log`, measurement `runs/ex06_merge.log` (19:58:23 UTC), restore `runs/ex06_restore.log`.
Store: Delta Lake on HDFS, `lake/zone_summary`, reset to version 0 (258 rows, one Parquet file of 7,543 bytes) by `d1_create.py`. Corrections (`src/e6_merge.py`): 7 rows, for zones 132, 161, 236, 237 (in the table) and 103, 104, 109 (not in the table, `runs/ex06_facts.log`). The correction values are hypothetical inputs chosen for the exercise.

Prediction: none. I filled the template with "No prediction written." in every field (`runs/ex06_prediction.txt`), so there is no prediction to compare.

Measured:

| Item | Value |
|---|---|
| Version, operation | 1, MERGE |
| Rows after | 261 |
| numSourceRows | 7 |
| numTargetRowsUpdated | 4 |
| numTargetRowsInserted | 3 |
| numTargetRowsCopied | 254 |
| numTargetFilesRemoved | 1 |
| numTargetFilesAdded | 1 |
| Data files in the table before / after | 1 / 1 |

Explanation of the copied rows from the number of files. At version 0 the table is one data file holding all 258 rows. Four of them match a correction and are updated; Delta never edits a Parquet file in place, so it rewrote that whole file: the 4 updated rows, the 258 - 4 = 254 untouched rows copied as they were, and the 3 inserted rows, giving one new file of 254 + 4 + 3 = 261 rows (7,605 bytes). Commit 1 removes the old file and adds the new one (1 removed, 1 added); the old file stays on HDFS for version 0. Afterwards the table was put back to the manual's state (version 0 + the Section 5 MERGE, 260 rows, `runs/ex06_restore.log`), because the final project keeps `lake/zone_summary`.

## Exercise 7: Find where the index pays (10 points)

Evidence: prediction `runs/ex07_prediction.txt` (committed f505589, 20:14:56 UTC), facts `runs/ex07_facts.log`, measurement `runs/ex07_index.log` (20:15:03 UTC), script `src/e7_index.js`.
Store: MongoDB 8.0.38, collection `bda.trips_jan`, 2,724,143 documents (January trips). `executionTimeMillis` is the best of three explains after one warm-up; all three are in the log.

Prediction: none ("No prediction written." in every field).

| Query | Index state | nReturned | totalKeysExamined | totalDocsExamined | executionTimeMillis (best) | Plan |
|---|---|---|---|---|---|---|
| zone 132 | `_id` only | 136,998 | 0 | 2,724,143 | 594 | COLLSCAN |
| zone 132 on 15 Jan | `_id` only | 5,379 | 0 | 2,724,143 | 632 | COLLSCAN |
| all trips on 15 Jan | `_id` only | 71,497 | 0 | 2,724,143 | 716 | COLLSCAN |
| zone 132 | + `{PULocationID: 1, pickup_date: 1}` | 136,998 | 136,998 | 136,998 | 136 | IXSCAN (compound) |
| zone 132 on 15 Jan | + compound | 5,379 | 5,379 | 5,379 | 5 | IXSCAN (compound) |
| all trips on 15 Jan | + compound | 71,497 | 0 | 2,724,143 | 711 | COLLSCAN |

The compound index served the first two queries and not the third: both of the first two filter on `PULocationID`, the index's first field, while the third filters only on `pickup_date`, its second field, and the plan stayed a COLLSCAN.

Index added for the third query: `{pickup_date: 1}`. With it, "all trips on 15 Jan" examined 71,497 keys and 71,497 documents and took 45 ms (best), against 716 ms with no index (the first two queries kept the compound index: 139 ms and 5 ms).

Index sizes (`db.trips_jan.stats().indexSizes`, bytes): `_id_` 30,420,992; `PULocationID_1_pickup_date_1` 22,065,152; `pickup_date_1` 13,996,032.

Compared with Section 8. In Section 8, on 258 documents, the index cut the documents examined from 258 to 1, but the median lookup moved only from 0.491 ms to 0.425 ms (`runs/sec8_lookup.log`), within the spread. Here, on 2,724,143 documents, the same idea cut the documents examined for zone 132 on 15 January from 2,724,143 to 5,379 and the time from 632 ms to 5 ms, so the index pays once a scan of the whole collection costs far more than the lookup itself.

## Exercise 8: Append instead of overwrite (10 points)

Evidence: prediction `runs/ex08_prediction.txt` (committed b471166, 20:24:10 UTC), script `src/e8_append.py`, outputs `runs/ex08_parquet.log`, `runs/ex08_delta.log`, `runs/ex08_parquet64.log`, `runs/ex08_delta64.log`. A first Parquet attempt at 20:24:17 failed before writing anything because HDFS refused the connection (`runs/ex08_parquet_fail1.log`); it was run once more at 20:25:40.
Store: Parquet directory and Delta table on HDFS (`lake/append_parquet`, `lake/append_delta`). Dataset: January trips (`curated`, 2,724,143 rows) plus the short trips (trip_distance <= 2, 1,596,003 rows) appended six times. A whole table has 2,724,143 + k x 1,596,003 rows, k = 0..6.

Prediction: none ("No prediction written." in every field); no second prediction for the 64-file runs.

| Run | Reads | Saw a whole table | Saw anything else |
|---|---|---|---|
| Parquet, files as Spark writes them | 57 | 56 | 1 (11,883,832 rows) |
| Delta, files as Spark writes them | 15 | 15 | 0 |
| Parquet, 64 files per append | 33 | 27 | 6 (3,347,588; 5,217,909; 7,013,410; 8,659,287; 10,205,416; 11,801,419 rows) |
| Delta, 64 files per append | 29 | 29 | 0 |

(Six appends took 10 s, 15 s, 54 s and 52 s respectively.)

Every partial count in the Parquet runs lies between two whole sizes (for example 11,883,832 is between k = 5, 10,704,158, and k = 6, 12,300,161), so the reader saw some of one append's files but not all. No read returned 0 rows or an error, unlike the overwrite in Section 6 (`runs/sec6_parquet.log`: 146 reads of 0 rows, 3 FileNotFoundException), because appending deletes nothing.

Difference between the two Parquet runs (1 partial read of 57 against 6 of 33). Explanation, not directly measured: a Parquet append becomes visible file by file as Spark moves each task's output into the directory when the job commits, and a reader that lists the directory in the middle of that sees only part of the batch. With 64 files per append there are more files to move, so each append spends longer in a half-visible state, and a larger share of reads fell into it. The number of files Spark wrote per append in the default run was not logged, so this is a hypothesis. Delta saw only whole tables in both runs because a new version exists only once its commit file appears in `_delta_log`.

## Exercises 9-10
Not yet done.
