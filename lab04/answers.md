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

## Exercises 3-10
Not yet done.
