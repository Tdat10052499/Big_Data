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

## Exercise 2: Profile March (5đ)
- Số dòng:
- Số cột:
- Schema giống tháng 1? Cột khác:
- Evidence:

## Exercise 3: The null pattern in another month (5đ)
- Số null theo cột (February):
- Số dòng null đồng thời ở tất cả cột bị ảnh hưởng:
- Kết luận (một quần thể hay không) và cách chứng minh:
- Evidence:

## Exercise 4: Round trip and verify (5đ)
- Lệnh dùng:
- Số dòng hai bản:
- Vì sao so sánh kích thước là chưa đủ:
- Evidence:

## Exercise 5: Change the block size and predict first (8đ)
- Prediction (recorded 2026-09-30 19:07:03 UTC, before any measurement): 4 blocks. The March file is 60,078,280 bytes and the target block size is 16,777,216 bytes (16 MB); 60,078,280 / 16,777,216 = 3.58, rounded up to 4. Reasoning: the file fills 3 full blocks (50,331,648 bytes) and the remaining 9,746,632 bytes need a fourth, partly filled block. (The reasoning sentence in `ex05_prediction.txt` was completed after the first timestamp but before loading the file with the 16 MB block size; the predicted number 4 did not change.)
- Measurement (`hdfs dfs -D dfs.blocksize=16777216 -put ...`, then `hdfs fsck ... -files -blocks -locations`, run 2026-09-30 19:16:49 UTC): 4 blocks, `blocksize=16777216`, replication 1, status HEALTHY. Block lengths: 16,777,216 / 16,777,216 / 16,777,216 / 9,746,632 bytes (sum = 60,078,280 bytes, equal to the file size).
- Do they agree? Yes. The measured block count (4) equals the prediction, and the last block (9,746,632 bytes) equals the remainder I computed beforehand. Reducing the block size from 128 MB to 16 MB turned the same 57.3 MB file from 1 block into 4, since each block holds at most the configured block size and the last one holds only the remainder.
- Evidence: `runs/ex05_prediction.txt`, `figures/ex05_prediction.png`, `figures/ex05_fsck_16mb.png`

## Exercise 6: The cost of the wrong curation rule (8đ)
- Số dòng mới / chênh lệch so với 2,724,143:
- Mean fare_amount (quy tắc gốc / quy tắc mới):
- Ngưỡng "đáng kể" (định nghĩa trước) và so với cái gì:
- Evidence:

## Exercise 7: Find a defect this manual did not list (8đ)
- Kiểm tra (code):
- Số dòng và tỷ lệ (nêu cohort):
- Vì sao là lỗi chứ không chỉ bất thường (2 câu):

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

## Exercise 9: Measure the cost of moving data (12đ)
- Ba lần `time hdfs dfs -put`:
- Throughput (MB/s):
- Thời gian Spark count:
- Claim (tối đa 3 câu) và một điều kiện làm claim sai:

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
