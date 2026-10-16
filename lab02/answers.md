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
