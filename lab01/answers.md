# Lab 1: Answers

> Mọi con số phải có bằng chứng (ảnh trong `figures/`, hoặc số trong `runs/run_record.md`).
> Mọi phần trăm phải nêu mẫu số (cohort).

## Exercise 1: Load February and report its blocks (5đ)
- Prediction / result: The February file was already in HDFS from the upload in Section 5.2, so it was not reloaded. Result measured with `hdfs fsck ... -files -blocks -locations` and `hdfs dfs -stat`.
- File size: 50,349,284 bytes (about 48.0 MiB, matching the `48.0 M` shown by `hdfs dfs -ls -h`)
- Number of blocks: 1 (confirm from the `1 block(s)` line in the fsck screenshot)
- Block size: 134,217,728 bytes (128 MB)
- Replication: 1
- Explanation (1 sentence): The file is smaller than the 128 MB block size, so HDFS does not need to split it and stores it as a single block that occupies only the file's own size on disk.
- Evidence: `figures/ex01_fsck_feb.png`

## Exercise 2 — Profile March

**Evidence:** `figures/ex02_03_profile.png` (script `src/ex02_03.py`)

March 2024 has **3,582,628 rows** and **19 columns**. The column names, their order and their Spark types are identical to January's (`jan.columns == mar.columns` is `True`; the list of columns differing in name or type is empty). The comparison covers names and Spark data types only; I did not compare nullability or Parquet metadata.

## Exercise 3 — The null pattern in February

**Evidence:** `figures/ex02_03_profile.png` (script `src/ex02_03.py`, same method as Section 7: null count per column, then the all-five-null count, here extended with a cross-check against `payment_type`)

February cohort: 3,007,526 rows. Five columns have missing values, each with the same count:

| Column | Null rows | Share of the 3,007,526 February rows |
|---|---|---|
| passenger_count | 185,610 | 6.172% |
| RatecodeID | 185,610 | 6.172% |
| store_and_fwd_flag | 185,610 | 6.172% |
| congestion_surcharge | 185,610 | 6.172% |
| Airport_fee | 185,610 | 6.172% |

Rows where all five are null: **185,610**. Rows with `payment_type = 0`: 185,610. Rows with all five null AND `payment_type = 0`: 185,610. Rows with all five null AND `payment_type != 0`: 0. Rows with `payment_type = 0` but NOT all five null: 0.

**Conclusion.** The single-population pattern found in January (140,162 rows = 4.728% of the 2,964,624 January rows; all five null exactly on the `payment_type = 0` rows) also holds in February: the five null sets coincide with each other and with the `payment_type = 0` set, because both set differences are empty. What does not carry over is the size: 6.172% of February rows versus 4.728% of January rows. The structure repeats, the rate does not.

**Limit.** Two months only; this shows that the null rows and the `payment_type = 0` rows coincide, not why the source system leaves those fields empty.

## Exercise 4: Round trip and verify (5 points)
- Commands (run 2026-09-30 20:23:20 UTC): `hdfs dfs -get /user/tdat1/nyc/curated /home/tdat1/bda/tmp_ex04/curated`, then `hdfs dfs -put /home/tdat1/bda/tmp_ex04/curated /user/tdat1/nyc/curated_copy`. Comparison: `hdfs dfs -du -s` on both paths for size, and a Spark job (`src/ex04_compare.py`, `spark.read.parquet(...).count()` on each path) for the row count. Note: the row count is obtained with Spark, not with an HDFS shell command; the only HDFS command I used to compare the two copies is `hdfs dfs -du -s` (and `-ls` to list the files).
- Rows in the two copies: `/user/tdat1/nyc/curated` = 2,724,143 rows; `/user/tdat1/nyc/curated_copy` = 2,724,143 rows; equal. Sizes: both 60,289,030 bytes (22,563,953 + 22,362,651 + 15,362,426 bytes in the three `part-*` files, the same in both directories).
- Why a size comparison alone is not enough for a Parquet dataset: a size is a byte count, not a record count. Parquet stores data column by column with compression and encoding, so two datasets with different rows can have the same number of bytes, and the same rows written with a different compression, encoding or row-group layout can have different sizes. Equal sizes therefore neither prove that the row counts match nor that the content matches. Here the sizes are equal because `-get` and `-put` copy the bytes unchanged, so equal size was expected; the row count obtained by reading the files is the independent check.
- Limit: matching row counts show that no rows were lost or added; they do not prove that every value is identical (that would need a checksum of the files, e.g. `hdfs dfs -checksum`, or a comparison of the data itself; neither was run here).
- Evidence: `figures/ex04_roundtrip.png`, `runs/logs/ex04_roundtrip.txt`, `src/ex04_compare.py`

## Exercise 5: Change the block size and predict first (8đ)
- Prediction (recorded 2026-09-30 19:07:03 UTC, before any measurement): 4 blocks. The March file is 60,078,280 bytes and the target block size is 16,777,216 bytes (16 MB); 60,078,280 / 16,777,216 = 3.58, rounded up to 4. Reasoning: the file fills 3 full blocks (50,331,648 bytes) and the remaining 9,746,632 bytes need a fourth, partly filled block. (The reasoning sentence in `ex05_prediction.txt` was completed after the first timestamp but before loading the file with the 16 MB block size; the predicted number 4 did not change.)
- Measurement (`hdfs dfs -D dfs.blocksize=16777216 -put ...`, then `hdfs fsck ... -files -blocks -locations`, run 2026-09-30 19:16:49 UTC): 4 blocks, `blocksize=16777216`, replication 1, status HEALTHY. Block lengths: 16,777,216 / 16,777,216 / 16,777,216 / 9,746,632 bytes (sum = 60,078,280 bytes, equal to the file size).
- Do they agree? Yes. The measured block count (4) equals the prediction, and the last block (9,746,632 bytes) equals the remainder I computed beforehand. Reducing the block size from 128 MB to 16 MB turned the same 57.3 MB file from 1 block into 4, since each block holds at most the configured block size and the last one holds only the remainder.
- Evidence: `runs/ex05_prediction.txt`, `figures/ex05_prediction.png`, `figures/ex05_fsck_16mb.png`

## Exercise 6: The cost of the wrong curation rule (8 points)
- Rule changed: keep rows where `passenger_count` is null (rows with `passenger_count = 0` are still dropped, and the other four Lab 1 rules still apply). Script `src/ex06_rule.py`, run 2026-09-30 20:28:24 UTC on the January file (cohort: the 2,964,624 January rows).
- Row counts: rule A (Lab 1, drop nulls) keeps 2,724,143 rows (equal to the Lab 1 curated count); rule B (keep nulls) keeps 2,839,382 rows. Difference: +115,239 rows = 4.230% of the 2,724,143 rule A rows, or 3.887% of the 2,964,624 January rows. These are exactly the rows with null `passenger_count` that pass the other four rules (115,239); the other 140,162 - 115,239 = 24,923 null rows fail at least one of the other rules.
- Mean `fare_amount`: rule A = 18.4394 USD; rule B = 18.5173 USD; difference = +0.0778 USD (+0.422% of the rule A mean). The 115,239 added rows have a mean fare of 20.3573 USD (10.4% above the rule A mean), so the overall mean moves only by about 4.06% (their share of rule B rows) x 1.92 USD = 0.078 USD.
- Is the change material? [DRAFT, decide and justify in your own words] For the mean fare, I judge it small: it is 0.422% of the mean, 0.0045 of the rule A standard deviation (17.4356 USD), and smaller than the difference between months' means under rule A (January 18.4394 USD, February 18.3807 USD, March 19.1536 USD; each month's own cohort): it is about the same size as January versus February (0.0587 USD) and about one ninth of January versus March (0.7142 USD). I am comparing against the natural month-to-month variation of the same statistic and against the spread of fares within January.
- Limits: this checks one statistic (the mean fare). The added rows are not a random sample: their mean fare is 10.4% higher and they are the `payment_type = 0` rows, so keeping them would change other statistics differently (for example anything that uses `passenger_count`, which is null in every added row). The month comparison is only a yardstick: the months may differ for reasons unrelated to curation.
- Evidence: `figures/ex06_rule.png`, `runs/logs/ex06_rule.txt`, `src/ex06_rule.py`

## Exercise 7: Find a defect this manual did not list (8 points)
- Defect: `total_amount` does not equal the sum of its itemised components. Check (`src/ex07_final.py`, run 2026-09-30 20:39:07 UTC): on the rows where `congestion_surcharge` and `Airport_fee` are both non-null, flag `abs(total_amount - (fare_amount + extra + mta_tax + tip_amount + tolls_amount + improvement_surcharge + congestion_surcharge + Airport_fee)) > 0.01`. The 0.01 tolerance is my choice (it absorbs floating-point noise such as -2.4999999999999964).
- Count and cohort: **635,915 rows = 22.515% of the 2,824,462 January rows** that have non-null `congestion_surcharge` and `Airport_fee`. My first scan (`src/ex07_scan.py`) treated those two nulls as 0 and gave 761,838 rows = 25.698% of all 2,964,624 January rows; I discarded that version as the headline because, for the 140,162 rows with null surcharges, the zero-fill is itself an assumption that can create a mismatch.
- What the mismatch looks like: in 590,944 of the 635,915 mismatched rows (92.928%) `total_amount` is lower than the components by exactly the congestion surcharge (diff = -2.5 in 591,045 rows); other common differences are -4.25 (22,451 rows) and -1.75 (18,149 rows). Net sum of (`total_amount` - components) over the mismatched rows: -1,594,137.45 USD (sum of absolute differences 1,615,447.15 USD). It is concentrated by vendor: VendorID 1 has 631,480 mismatches in 681,277 cohort rows (92.691% of that vendor's cohort rows); VendorID 2 has 4,435 in 2,143,185 (0.207%).
- Why it is a defect and not merely an unusual value: [DRAFT, rewrite in your own words] An unusual value is a rare but internally consistent record, whereas here the same record states itemised charges that add up to a different amount than the total charged, and it does so in over a fifth of the cohort rows. The discrepancy is systematic (one dominant amount, equal to a named component, concentrated in one vendor) rather than scattered, which points to a recording or definition difference and means any revenue figure built from `total_amount` or from the components would disagree by about 1.59 million USD net over these rows.
- Limits: (1) The rule "total = sum of components" is an assumption taken from the TLC data dictionary, not from the lab manual [cite the dictionary page you checked]; the data cannot tell whether `total_amount` or a component is the wrong one. (2) The vendor concentration is an association; I did not test its cause. (3) The differences -4.25 and -1.75 are consistent with a 1.75 USD airport fee also missing from the total, but I did not test that. (4) Side observation, not part of the count: in the 140,162 rows where `congestion_surcharge` and `Airport_fee` are null, `total_amount` exceeds the itemised charges by 2.5 USD in 125,088 rows (89.245% of those 140,162 rows, computed from the log), i.e. those rows carry a charge that is not itemised.
- Evidence: `figures/ex07_scan.png`, `figures/ex07_total_diag.png`, `figures/ex07_final.png`; `runs/logs/ex07_scan.txt`, `ex07_total_diag.txt`, `ex07_final.txt`; code `src/ex07_scan.py`, `src/ex07_total_diag.py`, `src/ex07_final.py`

## Exercise 8 — DataNode failure

**Evidence:** `figures/ex08_before.png`, `figures/ex08_during.png`, `figures/ex08_after.png`, `runs/logs/ex08_after.txt`

**Before (2026-09-30, prior to the kill).** `jps` listed NameNode (PID 2137), SecondaryNameNode (2554) and DataNode (2282). `hdfs dfs -cat /user/tdat1/nyc/ref/taxi_zone_lookup.csv` printed the CSV header (`"LocationID","Borough","Zone","service_zone"`) and the first rows. `hdfs dfsadmin -report` showed `Live datanodes (1)`.

**During (19:27 UTC, DataNode process stopped).** `jps` showed only NameNode and SecondaryNameNode. `hdfs dfs -ls /user/tdat1/nyc/ref` still succeeded: `-rw-r--r-- 1 tdat1 supergroup 12331 2026-09-30 18:16 /user/tdat1/nyc/ref/taxi_zone_lookup.csv` (the `1` is the replication factor).

Exact error of `hdfs dfs -cat`:

```
org.apache.hadoop.hdfs.BlockMissingException: Could not obtain block: BP-1007978907-127.0.1.1-1790790094014:blk_1073741829_1005 file=/user/tdat1/nyc/ref/taxi_zone_lookup.csv No live nodes contain current block Block locations: DatanodeInfoWithStorage[127.0.0.1:9866,DS-542c9b39-cc8f-4ed6-919c-6a861e32d40e,DISK] Dead nodes: DatanodeInfoWithStorage[127.0.0.1:9866,DS-542c9b39-cc8f-4ed6-919c-6a861e32d40e,DISK]
cat: Could not obtain block: ...
```

Earlier in the same run the client also logged `java.net.ConnectException: Connection refused` when it tried to open a block reader to the DataNode.

**Live nodes per `dfsadmin -report`.** At 19:27:02 UTC, i.e. shortly after the kill, the report still showed `Live datanodes (1)`.

**After (DataNode restarted with `hdfs --daemon start datanode`, checked at 19:33:05 UTC).** `jps` lists DataNode again with a new PID (11447; the old one was 2282) next to NameNode (2137) and SecondaryNameNode (2554). `hdfs dfs -cat /user/tdat1/nyc/ref/taxi_zone_lookup.csv` printed the CSV header and first rows again, and `hdfs dfsadmin -report` showed `Live datanodes (1)`. The block data had stayed on the DataNode's disk, so nothing was lost; only availability was interrupted.

**Explanation (link to §6.2).** [DRAFT — rewrite in your own words.] HDFS separates metadata from data. The NameNode keeps the namespace and the mapping file → block → DataNode location, in memory. The DataNode stores the block bytes on local disk. `-ls` only needs the NameNode, so it kept working. `-cat` needs the NameNode for the block location and then the DataNode for the bytes; the client received the location 127.0.0.1:9866, could not connect, and raised `BlockMissingException` because no other node holds a copy. The replication factor is 1 (visible in the `-ls` output) and the cluster has a single DataNode, so this block has no replica to fall back on. With replication ≥ 2 on several DataNodes, the client would read from another replica.

The report still showed one live node because the NameNode declares a DataNode dead only after missed heartbeats exceed a timeout (about 10.5 minutes with default settings: 2 × recheck-interval 300 s + 10 × heartbeat 3 s). The report therefore lags behind the real state for some minutes; I did not wait for the dead state, so that part is not observed here.

**Limits.** Single run on a pseudo-distributed cluster with one DataNode and one small file (one block). It shows that data availability depends on DataNode availability when replication = 1; it does not measure how long re-replication or recovery takes.

## Exercise 9: Measure the cost of moving data (12 points)
- Move (`time hdfs dfs -put` of the three Parquet files, 160,389,205 bytes, from a clean target path `/user/tdat1/nyc/ex09`, 2026-09-30 19:39:42 UTC): run 1 = 3.083 s (52.0 MB/s), run 2 = 2.319 s (69.2 MB/s), run 3 = 2.189 s (73.3 MB/s); mean 2.530 s. Throughput = 160.39 MB / time, MB = 10^6 bytes; it includes JVM start-up of the `hdfs` client. (An earlier series made during setup, `figures/data_04_upload_times.png`, gave 2.093 / 2.254 / 2.536 s for the same data.)
- Count (`spark-submit src/ex09_count.py`, `local[4]`, 2026-09-30 20:42:48 UTC, same three files already in HDFS `/user/tdat1/nyc/raw`, 9,554,778 rows = 2,964,624 + 3,007,526 + 3,582,628): wall-clock of the whole command 19.824 s / 20.121 s / 19.750 s. Inside the process: session start 1.464 / 1.333 / 1.348 s; reading the schema 3.082 / 2.965 / 2.953 s; first `count()` 1.591 / 1.623 / 1.582 s; second `count()` in the same session 0.252 / 0.291 / 0.266 s; `sum(fare_amount)` (forces reading one column) 0.796 / 0.801 / 0.832 s.
- Claim (at most three sentences) [DRAFT, rewrite in your own words]: On this single machine, loading the 160,389,205 bytes into HDFS took 2.189 to 3.083 s (52.0 to 73.3 MB/s), while the `spark-submit` command that counts the same data took 19.750 to 20.121 s wall-clock, of which the first count itself took only 1.58 to 1.62 s (0.25 to 0.29 s when repeated in the same session). For data this small, the fixed cost of starting Spark, not of reading the data, therefore dominates the count command. This conclusion would not hold for data much larger than 160 MB, where the time spent reading would grow past the fixed start-up cost.
- Limits: one machine with `local[4]`, three runs per measurement, operating-system cache not controlled. The two commands do different work (moving bytes versus reading and counting records), so the comparison is about cost on this setup, not about which system is faster. I did not itemise the roughly 12.6 s of the `spark-submit` wall-clock that is outside the timed sections (19.8 s wall-clock minus about 6.1 s for session start, schema read and first count, averaged, computed from the log). `count()` may not read every byte of the files; the `sum(fare_amount)` over one column took 0.80 to 0.83 s against 0.25 to 0.29 s for the repeated count, which is consistent with that but does not prove it.
- Evidence: `figures/ex09_put_timing.png`, `runs/logs/ex09_put_timing.txt`, `figures/ex09_spark_count.png`, `runs/logs/ex09_count.txt`, `src/ex09_count.py`

## Exercise 10: A profile that supports a decision (12đ)
- Top 5 zone và số chuyến:
- Biểu đồ: `figures/`
- Hạn chế quan trọng nhất:

## Exercise 11: Reproducibility, tested by someone else (12đ)
- Số đã tái tạo và có khớp không:
- Thông tin còn thiếu trong record của bạn đó:
- Điều đã sửa trong record của mình:

## Exercise 12: Defend the treatment of the 140,162 rows (12đ)
1. Quyết định:
2. Bằng chứng (số của riêng bạn):
3. Điều kiện quyết định này đúng:
4. Điều kiện sẽ quyết định ngược lại:
5. Điều bằng chứng chưa chứng minh được:
