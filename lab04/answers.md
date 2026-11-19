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

## Exercises 5-10
Not yet done.
