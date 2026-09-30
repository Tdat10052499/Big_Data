# Run record: Lab 1 (Data Quality and HDFS)

Student: Hồ Du Tuấn Đạt - 2374802010097
Linux account: tdat1 (machine DESKTOP-ANNRGGL)

Conventions: every number below is read from a screenshot in `lab01/figures/`, from a file in the repo (`lab01/runs/`, `lab01/out/`), or computed from those numbers (the formula is given). Items marked `[FILL IN]` are information that has no evidence yet and must be confirmed or measured by me. Every percentage names its denominator (cohort). All timestamps are UTC on 2026-09-30 unless stated otherwise.

## 1. Environment

| Component | Measured value | Evidence |
|---|---|---|
| Windows / WSL | WSL2, distro named `Ubuntu` (VERSION 2, Running); a `docker-desktop` distro is also present (Stopped) | `figures/setup_01_wsl_list.png` |
| OS (Linux) | Ubuntu 26.04 LTS | `setup/versions.txt`; `runs/data_card.json` (`software.os`) |
| Java | OpenJDK 17.0.20.1 (2026-08-18), 64-Bit Server VM | `figures/setup_02_java_version.png` |
| Hadoop | 3.4.3 (compiled Fri Feb 13 2026, branch-3.4.3) | `figures/setup_08b_namenode_ui.png` |
| Spark | 3.5.9 (Scala 2.12.18, OpenJDK 17.0.20.1) | `figures/setup_10_hdfs_hello_spark.png` |
| Python (venv `~/bda/venv`) | 3.14.4; PySpark 3.5.9, pandas 3.0.6, pyarrow 25.0.1, JupyterLab 4.6.4 | `setup/versions.txt`; `pip list` output (see issue 8) |

### Deviations from the manual (declared honestly)
- The manual uses Ubuntu 24.04 LTS and Python 3.12; my machine runs Ubuntu 26.04 LTS and Python 3.14.4.
- Reason: [FILL IN: for example, the default `Ubuntu` distro was already installed on the machine and I did not install `Ubuntu-24.04` specifically].
- Risk that was checked: PySpark 3.5.9 runs the DataFrame API on Python 3.14.4 (`p1_schema.py` read January: 2,964,624 rows, 19 columns, schema contains `timestamp_ntz`; `p2`, `p3`, `p4` and `ex02_03.py` ran successfully). However, `DataFrame.toPandas()` **does not work**: `ModuleNotFoundError: No module named 'distutils'` (PySpark 3.5.9 imports `distutils`, which was removed from the standard library in Python 3.12). The lecturer's code does not use `toPandas`; when I need to bring small data into Python I use `.collect()`.
- Because the versions differ, my measurements may differ from the numbers printed in the manual; I report only numbers measured on my own machine.

Installation method: manual, following §3 (no `setup-bda.sh`, no Docker Compose).
Mode: pseudo-distributed on one machine, replication = 1, block size = 134217728 bytes (128 MB).

## 2. Deliberate security choices

- SSH uses an RSA key with an empty passphrase (`ssh-keygen -P ''`) so that the Hadoop start-up scripts run without a password prompt. Checked with `ssh ... localhost 'echo IT WORKS'` (`figures/setup_03_ssh_it_works.png`).
- This choice is acceptable only on a personal machine used for the course; it is not acceptable on a shared or Internet-facing machine.
- `dfs.permissions.enabled = true` and `dfs.namenode.acls.enabled = true` were left unchanged because Lab 4 needs access control.

## 3. Configuration applied

- The four XML files (core-site, hdfs-site, mapred-site, yarn-site) are stored in `setup/`. **Note:** `mapred-site.xml` and `yarn-site.xml` were initially empty templates (`<configuration>` with no `<property>`; captured with `sed -n '/<configuration>/,$p'` on the real files in `/opt/hadoop/etc/hadoop/`), so they passed the XML syntax check (`figures/setup_06_xml_valid.png`) but were missing their settings. I filled both files following §3.7.3 of the manual during the §10 step (see issue 9); the backups are `*.xml.bak`. The copies in `setup/` have been updated: [FILL IN: pushed the new `setup/mapred-site.xml` and `setup/yarn-site.xml`: yes/no].
- Environment variable block: `setup/bashrc_snippet.sh` (one 8-line export block; the stray `EOF` line and the duplicated block were removed).
- The `*_USER` lines in `hadoop-env.sh`: changed from `<tdat1>` to `tdat1` (see issue 1). [FILL IN: confirm that all 5 lines were checked and `bash -n` printed `SYNTAX OK`: yes/no]
- Spark: `spark-env.sh` sets `JAVA_HOME` and `HADOOP_CONF_DIR`; log level set to `warn`. Spark runs in `local[4]`; `spark.sql.shuffle.partitions` = 200 (the default, not set in `spark-defaults.conf`; measured with `s.conf.get("spark.sql.shuffle.partitions")`).

## 4. Provenance of data and code

- Data: NYC TLC Yellow Taxi trip records for January, February and March 2024, and `taxi_zone_lookup.csv`. Downloaded at 2026-09-30 18:11 (`figures/data_01_download_ls.png`).
- Source file sizes (bytes): `taxi_zone_lookup.csv` = 12331. The three Parquet files: 49,961,641 (01), 50,349,284 (02), 60,078,280 (03) bytes (`figures/ex09_put_timing.png` lists all three sizes in HDFS; total 160,389,205 bytes).
- SHA-256 checksums (recorded before loading into HDFS; `figures/data_02_checksums.png`, `runs/source_checksums.txt`):

| File | SHA-256 |
|---|---|
| yellow_tripdata_2024-01.parquet | c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510 |
| yellow_tripdata_2024-02.parquet | c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20 |
| yellow_tripdata_2024-03.parquet | 2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823 |
| taxi_zone_lookup.csv | 1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed |

- Raw data: `~/bda/data/raw` (outside the repo); in HDFS: `/user/tdat1/nyc/raw` and `/user/tdat1/nyc/ref`; curated dataset: `/user/tdat1/nyc/curated` (not included in the submission archive, not deleted).
- Code in `lab01/src/`: `p1_schema.py` to `p5_card.py` and `lab01_profile.ipynb` were supplied by the lecturer in `Lab01_student_files.zip`; `ex02_03.py` is the script I ran for Exercises 2 and 3 (a draft written by an AI assistant; I ran it and checked its output).
- Changes I made to the lecturer's code:
  - Changed the HDFS paths `/user/thaianh/...` to `/user/tdat1/...` in `p1_schema.py`, `p2_nulls.py`, `p3_defects.py`, `p4_curate.py`, `p5_card.py` and `lab01_profile.ipynb` (`sed -i`; the check `grep -rn thaianh lab01/src/` printed `OK: no thaianh left`; commit `7559a68`).
  - `p5_card.py`: removed the final `spark.stop()` line (the script never creates `spark`, so it would raise `NameError` after the file is already written); replaced `"shuffle_partitions": 8` (a value pre-filled in the manual, with no evidence) with 200 (the measured value, see §3); replaced the `ai_assistance` field (see §8).
- The scripts `bda-start.sh`, `bda-status.sh`, `bda-stop.sh` were supplied by the lecturer and are not in the repo.

## 5. Measurements

### 5.1. Loading data into HDFS (§5.2)
- Sizes in HDFS (`hdfs dfs -ls -h`, `figures/data_03_hdfs_ls_time.png`):

| File | Size | Replication |
|---|---|---|
| yellow_tripdata_2024-01.parquet | 47.6 M | 1 |
| yellow_tripdata_2024-02.parquet | 48.0 M | 1 |
| yellow_tripdata_2024-03.parquet | 57.3 M | 1 |
| taxi_zone_lookup.csv (`nyc/ref`) | 12.0 K | 1 |

- Total size of `nyc/raw` (`hdfs dfs -du -s`): 160389205 bytes (152.96 MiB, 160.39 MB). The second column (which includes replication) is also 160389205 because replication = 1.

### 5.1b. Upload timing, series 1 (three runs from a clean path, during setup)
- Command (each run: `hdfs dfs -rm -r -f -skipTrash /user/tdat1/nyc/raw`, `mkdir -p`, then put): `time hdfs dfs -put ~/bda/data/raw/*.parquet /user/tdat1/nyc/raw/`
- Evidence: `figures/data_04_upload_times.png`

| Run | `real` | Throughput (160.39 MB / real) |
|---|---|---|
| 1 | 2.093 s | 76.6 MB/s |
| 2 | 2.254 s | 71.2 MB/s |
| 3 | 2.536 s | 63.2 MB/s |

- Mean 2.294 s; fastest 2.093 s; slowest 2.536 s. Throughput = total bytes / `real`, so it is the effective throughput of the whole command (including JVM start-up), not the raw write speed of HDFS.

### 5.2. Cluster state after loading
- NameNode page (`figures/setup_08b_namenode_ui.png`): Live Nodes = 1, Dead Nodes = 0, Safemode off; 17 files and directories, 5 blocks; DFS Used = 154.21 MB (0.01%); Configured Capacity = 1006.85 GB; Started Thu Oct 01 01:13:29 +0700 2026.
- `bda-status.sh` (`figures/setup_11_bda_status.png`): NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer and sshd `UP`; Spark History Server, Kafka and MongoDB `DOWN` (not installed, not needed for Lab 1); Live datanodes (1); Safe mode OFF.

### 5.3. Blocks and storage (§6)
- Command: `hdfs fsck /user/tdat1/nyc/raw -files -blocks` (`figures/hdfs_01_fsck_raw.png`, the last 25 lines of the output, at 18:29:04).
- Result: `The filesystem under path '/user/tdat1/nyc/raw' is HEALTHY`; Minimally replicated blocks: 3 (100.0 %); Over/Under/Mis-replicated blocks: 0; Default replication factor: 1; Average block replication: 1.0; Missing blocks: 0; Corrupt blocks: 0; Missing replicas: 0 (0.0 %).
- Number of blocks: 3 blocks for 3 files, i.e. one block per file. Explanation: each file is smaller than the 128 MB block size (47.6, 48.0, 57.3 MB), so HDFS does not need to split it. For the February file specifically, `fsck -files -blocks -locations` lists `1 block(s)` (see §5.9).
- Storage directories (`figures/hdfs_02_nn_vs_dn.png`):
  - NameNode (`du -sh /opt/hadoop/data/nn`): 2.1M; it holds small metadata: `VERSION` (213 bytes), `edits_*` files (one is 1048576 bytes).
  - DataNode (`du -sh /opt/hadoop/data/dn`): 155M; it holds the files `blk_1073741825` to `blk_1073741829` (5 blocks, matching "5 blocks" on the NameNode page: 3 Parquet, 1 CSV, 1 `hello.txt`). The original file names do not appear in this directory.
  - DataNode / NameNode ratio: about 73.8 times (155 / 2.1; it uses the rounded `du -sh` values, so it is only an estimate).
- Interpretation (a draft based on the measurements; I must re-read and edit it so it says what I mean): the NameNode directory is small (2.1M) but it is the only thing that links the anonymous `blk_*` blocks to file names, so I would back up the NameNode directory more often; the DataNode directory is about 74 times larger, so backing it up is expensive and, on a real cluster, it is protected by replication instead. Limit: there are only 5 files, so this ratio cannot be extrapolated to a large cluster (NameNode size depends on the number of files and blocks, not on the number of bytes).

### 5.4. MapReduce (§10)
- Command: `hadoop jar hadoop-mapreduce-examples-3.4.3.jar wordcount /user/tdat1/nyc/ref/taxi_zone_lookup.csv /user/tdat1/nyc/out/wordcount`, run on YARN as `job_1790798937946_0001` (20:09:53 to 20:10:08, SUCCEEDED, 1 map, 1 reduce, elapsed 9 s). Evidence: `figures/sec10_wordcount_counters.png`, `figures/sec10_jobhistory.png`, `runs/logs/sec10_wordcount.txt`.
- Map input / Map output / Reduce input / Reduce output records: 266 / 834 / 425 / 425. Combine input / output records: 834 / 425.
- Ratio map output / reduce input: 834 / 425 = 1.96.
- Interpretation: the combiner merged 409 of the 834 map output records (409 / 834 = 49.0%) before the shuffle, so only 425 key-value pairs had to be moved to the reducer. Limit: one input split, a 12,331-byte file, one machine (the shuffle is a memory copy); the saving on a larger input or on a real cluster was not measured here.
- The first run (`runs/logs/sec10_wordcount_localrunner.txt`, `job_local2007299301_0001`, 20:03:47) used LocalJobRunner because `mapred-site.xml` and `yarn-site.xml` were still empty (issue 9); its counters were identical (266 / 834 / 425 / 425).

### 5.5. Profiling January (§7.2, §7.3, §7.4)
- January rows: 2,964,624; columns: 19 (`p1_schema.py`). The row count appears as the cohort in `figures/sec8_p3_defects.png`, `figures/sec11_p4_curate.png` and `figures/ex02_03_profile.png`; the 19 columns follow from March having 19 columns and `jan.columns == mar.columns` being `True` (`figures/ex02_03_profile.png`).
- Missing values (`p2_nulls.py`, 19:45:34; `figures/sec7_p2_nulls.png`): the five columns `passenger_count`, `RatecodeID`, `store_and_fwd_flag`, `congestion_surcharge` and `Airport_fee` each have 140,162 missing values (4.73%, i.e. 4.728% of the 2,964,624 January rows). Rows where all five columns are null together: 140,162.
- `payment_type` distribution (January): 0 = 140,162; 1 = 2,319,046; 2 = 439,191; 3 = 19,597; 4 = 46,628 (total 2,964,624).
- Conclusion about the population: the 140,162 rows where all five columns are null equal the 140,162 rows with `payment_type = 0`; I checked the intersection of the two sets in §5.8 (both set differences are 0), so it is one set of rows, not five separate ones.

### 5.6. January quality report (§8)
`p3_defects.py` (19:45:58; `figures/sec8_p3_defects.png`, `out/quality_report.csv`). Observed ranges: pickup from 2002-12-31 22:59:39 to 2024-02-01 00:01:15; `fare_amount` from -899.0 to 5000.0; `trip_distance` from 0.0 to 312722.3. Denominator of every percentage: the 2,964,624 January rows.

| Check | Rows | Share (of the 2,964,624 January rows) |
|---|---|---|
| fare_amount below zero | 37,448 | 1.263% |
| fare_amount exactly zero | 893 | 0.030% |
| total_amount below zero | 35,504 | 1.198% |
| passenger_count is zero | 31,465 | 1.061% |
| passenger_count is null | 140,162 | 4.728% |
| trip_distance is zero | 60,371 | 2.036% |
| zero distance but fare above 0 | 56,569 | 1.908% |
| trip_distance above 100 miles | 59 | 0.002% |
| pickup outside January 2024 | 18 | 0.001% |
| dropoff before pickup | 56 | 0.002% |
| dropoff equal to pickup | 814 | 0.027% |
| unknown pickup zone 264 or 265 | 12,018 | 0.405% |
| exact duplicate rows | 0 | 0.000% |

### 5.7. Curated dataset (§11)
- `p4_curate.py` (19:51:05; `figures/sec11_p4_curate.png`, `runs/logs/p4_curate.txt`). Keep rules: `fare_amount >= 0`; `trip_distance > 0`; `passenger_count` not null and > 0; pickup inside January 2024; dropoff strictly after pickup.
- Raw 2,964,624; kept 2,724,143; dropped 240,481 = 8.11% of the 2,964,624 January rows. Added columns `pickup_date`, `pickup_hour`, `trip_minutes`.
- Output: `/user/tdat1/nyc/curated` (`_SUCCESS` and three Parquet files `part-00000`, `part-00001`, `part-00003` of 22,563,953 / 22,362,651 / 15,362,426 bytes according to `hdfs dfs -ls`). The kept row count is obtained by re-reading the output directory from HDFS.
- Data card: `runs/data_card.json` is written by `p5_card.py`; every hard-coded number in the script (140,162; 37,448; 56,569; 18; 0 duplicates; the pickup range; 2,724,143; 240,481; 8.11%) was checked against my own output in §5.5 to §5.7.

### 5.8. March and February (Exercises 2 and 3)
`ex02_03.py` (19:49:54; `figures/ex02_03_profile.png`, `runs/logs/ex02_03.txt`).
- March: 3,582,628 rows, 19 columns; column names and order are identical to January; no column differs in name or Spark type (nullability was not compared).
- February (cohort 3,007,526 rows): each of the five columns has 185,610 missing values (6.172% of the 3,007,526 February rows); all five null together: 185,610; `payment_type = 0`: 185,610; all five null AND `payment_type = 0`: 185,610; all five null AND `payment_type != 0`: 0; `payment_type = 0` AND NOT all five null: 0. The pattern holds in February (the two sets coincide), but the rate differs: 6.172% (February) versus 4.728% (January). The same intersection checks for January give 140,162 / 140,162 / 140,162 / 0 / 0.

### 5.9. Blocks: Exercises 1 and 5
- Exercise 1 (`figures/ex01_fsck_feb.png`, fsck at 19:10:47): the February file has size 50,349,284 bytes, block size 134,217,728, replication 1, 1 block, HEALTHY. Explanation: 50,349,284 < 134,217,728, so it fits in one block.
- Exercise 5: prediction recorded before measuring (`runs/ex05_prediction.txt`, 19:07:03; `figures/ex05_prediction.png`): the March file is 60,078,280 bytes, block size 16,777,216 bytes; 60,078,280 / 16,777,216 = 3.5809, rounded up = **4 blocks**. Measurement (`figures/ex05_fsck_16mb.png`, fsck at 19:16:49, after `-D dfs.blocksize=16777216 -put`): 4 blocks of 16,777,216 + 16,777,216 + 16,777,216 + 9,746,632 = 60,078,280 bytes; `blocksize=16777216`. Prediction and measurement **agree**. My first draft of the prediction file still had placeholders and a non-integer; I corrected and recorded it before the measurement (see issue 12).
- Temporary directory `/user/tdat1/nyc/ex05_bs16m` after the screenshots: [FILL IN: deleted with `hdfs dfs -rm -r -skipTrash`: yes/no].

### 5.10. Exercise 8: stopping the DataNode
Evidence: `figures/ex08_before.png`, `figures/ex08_during.png`, `figures/ex08_after.png`, `runs/logs/ex08_after.txt`.
- Before: jps shows NameNode 2137, SecondaryNameNode 2554, DataNode 2282; `-cat` reads `taxi_zone_lookup.csv`; `Live datanodes (1)`.
- During (19:27:00 to 19:27:02, DataNode killed with `kill`): jps shows only NameNode and SecondaryNameNode; `hdfs dfs -ls /user/tdat1/nyc/ref` still returns the file (12331 bytes, replication 1); `-cat` fails with `BlockMissingException: Could not obtain block: BP-1007978907-127.0.1.1-1790790094014:blk_1073741829_1005 ... No live nodes contain current block`; `dfsadmin -report` at 19:27:02 still reports `Live datanodes (1)` (the NameNode marks a DataNode dead only after about 10.5 minutes with the default configuration; the dead state was not observed).
- After (restarted with `hdfs --daemon start datanode`, 19:33:05): jps shows DataNode with a new PID 11447, `-cat` reads the CSV again, `Live datanodes (1)`.

### 5.11. Exercise 9: cost of moving data (part 1)
- `figures/ex09_put_timing.png`, `runs/logs/ex09_put_timing.txt` (19:39:42). Data: the three Parquet files, total 160,389,205 bytes. Each run: delete `/user/tdat1/nyc/ex09`, `mkdir -p`, `hdfs dfs -put` the three files; time measured with `time` (wall clock).

| Run | Time | Throughput (160.39 MB / time, MB = 10^6 bytes) |
|---|---|---|
| 1 | 3.083 s | 52.0 MB/s |
| 2 | 2.319 s | 69.2 MB/s |
| 3 | 2.189 s | 73.3 MB/s |

- Mean 2.530 s. Run 1 is the slowest; the cause (cache, JVM start-up, ...) has not been verified. Part 2 (time for Spark `count` on the same data in HDFS) and the claim of at most three sentences: [FILL IN: not measured yet].
- Directory `/user/tdat1/nyc/ex09` after the measurement: [FILL IN: deleted: yes/no].

## 6. Issues and how they were handled

| # | Issue | Cause | Detection and fix | Evidence |
|---|---|---|---|---|
| 1 | `syntax error near unexpected token 'newline'` at line 446 of `hadoop-env.sh` on every Hadoop command; services still started | Typed the placeholder `<tdat1>` (with angle brackets) literally for `HDFS_NAMENODE_USER`; bash reads `<...>` as a redirection | Read the error message; fixed with `sed`; checked with `bash -n`; restarted services without reformatting | `figures/setup_08_jps_six_services.png`, `figures/setup_10_hdfs_hello_spark.png` (taken before the fix) |
| 2 | `-bash: /opt/spark/sbin: No such file or directory` when loading `.bashrc` | The `export PATH=...` line was split in two while typing in nano | Rewrote the block with `cat >> ~/.bashrc <<'EOF'` on a single line | `figures/setup_02_java_version.png` (error line at the top of the image) |
| 3 | `tên_bạn: No such file or directory` when loading `.bashrc` | Left `<tên_bạn>` in `PYSPARK_PYTHON` | Replaced with `/home/tdat1/...` | (no screenshot) |
| 4 | `hdfs dfs` reports `Connection refused` at `localhost:9000` | The NameNode was not running because WSL stops all services when the last terminal closes (§4.1) | `sudo service ssh start`, `bda-start.sh hdfs yarn jobhistory`, `hdfs dfsadmin -safemode wait`; no reformat | (no screenshot) |
| 5 | `bda-status.sh` reports every service `DOWN` | Same cause as issue 4 | Restarted in the order above | (no screenshot) |
| 6 | `cp: target '.../lab01/src/': No such file or directory` | The `src` and `out` directories were missing from the clone (Git does not track empty directories) | `mkdir -p lab01/src lab01/out`, then copied again | (no screenshot) |
| 7 | `git push` rejected: `Password authentication is not supported` | GitHub does not accept the account password | Authenticated with [FILL IN: `gh auth login` or a token] | (no screenshot) |
| 8 | `ModuleNotFoundError: No module named 'distutils'` when calling `toPandas()` | PySpark 3.5.9 imports `distutils`, removed from the standard library since Python 3.12; the venv uses Python 3.14.4 | Kept Python 3.14.4; avoid `toPandas()` (use `.collect()` when needed); DataFrame API verified separately: `p1_schema.py` ran correctly (2,964,624 rows, 19 columns) | terminal output; `lab01/runs/logs/*` for `p2` to `p4` |
| 9 | The `wordcount` job ran with `LocalJobRunner` (`job_local2007299301_0001`) and did not appear in JobHistory | The real `mapred-site.xml` and `yarn-site.xml` were empty (missing `mapreduce.framework.name=yarn`, `mapreduce_shuffle`, ...) | Inspected the real files; filled them following §3.7.3; `stop-yarn.sh`/`start-yarn.sh`, restarted JobHistory; reran the job on YARN (`job_1790798937946_0001`) | `runs/logs/sec10_wordcount_localrunner.txt`, `figures/sec10_wordcount_counters.png`, `figures/sec10_jobhistory.png` |
| 10 | `git push` rejected (`fetch first`); `git pull --rebase` refused because of uncommitted changes | Screenshots and `answers.md` were edited in the GitHub web UI so the remote was ahead; the machine still had uncommitted edits and untracked files with the same names | Committed the edits in `lab01/src`; moved the duplicate files to `~/bak_untracked`; `git pull --rebase`; push succeeded (`4bb6668`) | (terminal) |
| 11 | `p5_card.py` wrote `"shuffle_partitions": 8` although Spark uses 200; the final `spark.stop()` line raises `NameError` | 8 is the sample value in the manual; the script never creates `spark` | Measured `spark.sql.shuffle.partitions` = 200; fixed the card; removed the `spark.stop()` line | `runs/data_card.json` |
| 12 | The Exercise 5 prediction file had unfilled placeholders, then a non-integer (3.58) | Ran the command that writes the file before filling everything in | Corrected to the integer 4 with the calculation, recorded at 19:07:03 (before the measurement at 19:16:49) | `runs/ex05_prediction.txt`, `figures/ex05_prediction.png` |

## 7. Data handling decisions

- Curation rules applied (Lab 01, `p4_curate.py`): `fare_amount >= 0`; `trip_distance > 0`; `passenger_count` not null and > 0; pickup inside January 2024; dropoff strictly after pickup.
- Rows kept / dropped / share dropped: 2,724,143 / 240,481 / 8.11% of the 2,964,624 January rows (§5.7).
- Among the 240,481 dropped rows are the 140,162 rows with null `passenger_count` (`payment_type = 0`, 4.728% of the 2,964,624 January rows; §5.5). I have not yet broken down the rest of the 240,481 rows by rule (the conditions overlap).
- The rule I would set differently and why (Exercises 6 and 12): [FILL IN]

## 8. AI assistance disclosure (draft, to be confirmed)

I used an AI assistant (Claude) to: read and summarise the Lab 1 manual; guide the order of the installation steps; explain errors and propose fixes (issues 1 to 12); draft the repo structure template; draft the script `ex02_03.py`; read the evidence screenshots in the repo; draft `run_record.md` and `answers.md` (including the draft interpretation in §5.3 and the Exercise 8 explanation); and draft the `ai_assistance` field in `data_card.json` (I used that text as written, without editing it).
How I verified it: [FILL IN: for example, I ran every command myself on my own machine, compared the output with the Expected output in the manual, and checked the numbers in this record against the original screenshots and log files].
Exercise answers and interpretations: [FILL IN: written by me / AI consulted for which parts]. Every number in this record comes from screenshots and files I produced on my own machine.

## 9. Remaining (not done at the time of this update)

Exercise 4 (get/put and row-count comparison), Exercise 6 (change the `passenger_count` rule), Exercise 7 (a defect not listed in Table 8), Exercise 9 Spark `count` part and claim, Exercise 10 (top five pickup zones, chart), Exercise 11 (exchange run records), Exercise 12 (defend the decision on the 140,162 rows); update `answers.md`, build `lab01_<student id>.zip`.
