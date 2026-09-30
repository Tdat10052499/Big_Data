# Run record: Lab 1 (Data Quality and HDFS)

Sinh viên: Hồ Du Tuấn Đạt - 2374802010097
Tài khoản Linux: tdat1 (máy DESKTOP-ANNRGGL)

Quy ước: mọi số dưới đây được đọc từ ảnh trong `lab01/figures/` hoặc từ file trong repo, hoặc được tính từ các số đó (có ghi công thức). Mục `[ĐIỀN]` là thông tin chưa có bằng chứng, cần tự xác nhận hoặc tự đo.

## 1. Môi trường

| Thành phần | Giá trị đo được | Bằng chứng |
|---|---|---|
| Windows / WSL | WSL2, distro tên `Ubuntu` (VERSION 2, Running); còn distro `docker-desktop` (Stopped) | `figures/setup_01_wsl_list.png` |
| OS (Linux) | Ubuntu 26.04 LTS | `setup/versions.txt`; build Java ghi `26.04-Ubuntu` (`figures/setup_02_java_version.png`) |
| Java | OpenJDK 17.0.20.1 (2026-08-18), 64-Bit Server VM | `figures/setup_02_java_version.png` |
| Hadoop | 3.4.3 (biên dịch Fri Feb 13 2026, branch-3.4.3) | `figures/setup_08b_namenode_ui.png` |
| Spark | 3.5.9 (Scala 2.12.18, OpenJDK 17.0.20.1) | `figures/setup_10_hdfs_hello_spark.png` |
| Python (venv `~/bda/venv`) | 3.14.4 | `setup/versions.txt` |

### Khác biệt so với tài liệu (khai báo trung thực)
- Tài liệu dùng Ubuntu 24.04 LTS và Python 3.12; máy của tôi chạy Ubuntu 26.04 LTS và Python 3.14.4.
- Lý do: [ĐIỀN: ví dụ, distro `Ubuntu` mặc định đã cài sẵn trên máy, tôi không cài đúng `Ubuntu-24.04`].
- Rủi ro: PySpark 3.5.x có thể không hỗ trợ Python 3.14. Cần kiểm tra khi chạy PySpark ở §7. Kết quả kiểm tra: [ĐIỀN sau khi chạy notebook ở §7].
- Số hiệu phiên bản khác nhau nên số đo có thể lệch so với số in trong tài liệu; tôi chỉ báo số đo trên máy của mình.

Cách cài: thủ công theo §3 (không dùng `setup-bda.sh`, không dùng Docker Compose).
Chế độ: pseudo-distributed trên một máy, replication = 1, block size = 134217728 byte (128 MB).

## 2. Lựa chọn bảo mật có chủ đích

- SSH dùng khóa RSA với passphrase rỗng (`ssh-keygen -P ''`) để các script khởi động Hadoop chạy không cần nhập mật khẩu. Kiểm tra bằng `ssh ... localhost 'echo IT WORKS'` (`figures/setup_03_ssh_it_works.png`).
- Lựa chọn này chỉ chấp nhận được trên máy cá nhân dùng cho môn học; không chấp nhận trên máy dùng chung hoặc máy hướng Internet.
- `dfs.permissions.enabled = true` và `dfs.namenode.acls.enabled = true` được giữ nguyên vì Lab 4 cần kiểm soát truy cập.

## 3. Cấu hình đã áp dụng

- Bốn file XML (core-site, hdfs-site, mapred-site, yarn-site) lưu tại `setup/`; cả bốn đều qua kiểm tra cú pháp XML (`figures/setup_06_xml_valid.png`).
- Khối biến môi trường: `setup/bashrc_snippet.sh` (một khối 8 dòng export, đã dọn dòng `EOF` thừa và khối lặp).
- Các dòng `*_USER` trong `hadoop-env.sh`: đã sửa từ `<tdat1>` thành `tdat1` (xem sự cố 1). [ĐIỀN: xác nhận đã kiểm tra cả 5 dòng và `bash -n` in `SYNTAX OK`: có/không]
- Spark: `spark-env.sh` đặt `JAVA_HOME` và `HADOOP_CONF_DIR`; mức log đặt `warn`.

## 4. Nguồn gốc dữ liệu và mã

- Dữ liệu: NYC TLC Yellow Taxi trip records, tháng 01, 02, 03/2024, và `taxi_zone_lookup.csv`. Tải lúc 2026-09-30 18:11 (`figures/data_01_download_ls.png`).
- Kích thước file nguồn (byte): `taxi_zone_lookup.csv` = 12331 (theo log `wget`). Ba file Parquet hiển thị 48M, 49M, 58M trong `ls -lh` (làm tròn lên).
- Checksum SHA-256 (đã ghi trước khi nạp HDFS, `figures/data_02_checksums.png`, `runs/source_checksums.txt`):

| File | SHA-256 |
|---|---|
| yellow_tripdata_2024-01.parquet | c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510 |
| yellow_tripdata_2024-02.parquet | c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20 |
| yellow_tripdata_2024-03.parquet | 2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823 |
| taxi_zone_lookup.csv | 1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed |

- Dữ liệu thô: `~/bda/data/raw` (ngoài repo); trong HDFS: `/user/tdat1/nyc/raw` và `/user/tdat1/nyc/ref`.
- Mã trong `lab01/src/` (`p1_schema.py` đến `p5_card.py`, `lab01_profile.ipynb`) do giảng viên cung cấp trong `Lab01_student_files.zip`.
- Phát hiện: các script `p1_schema.py`, `p2_nulls.py`, `p3_defects.py`, `p4_curate.py`, `p5_card.py` đặt sẵn đường dẫn HDFS `/user/thaianh/...`. Phải đổi thành `/user/tdat1/...` trước khi chạy. Tính đến commit `0868998` trong repo, các file này **chưa được sửa**. Các file tôi đã sửa: [ĐIỀN: liệt kê sau khi sửa].
- Script `bda-start.sh`, `bda-status.sh`, `bda-stop.sh` do giảng viên cung cấp, không đưa vào repo.

## 5. Đo đạc

### 5.1. Nạp dữ liệu vào HDFS (§5.2)
- Kích thước trong HDFS (`hdfs dfs -ls -h`, `figures/data_03_hdfs_ls_time.png`):

| File | Kích thước | Replication |
|---|---|---|
| yellow_tripdata_2024-01.parquet | 47.6 M | 1 |
| yellow_tripdata_2024-02.parquet | 48.0 M | 1 |
| yellow_tripdata_2024-03.parquet | 57.3 M | 1 |
| taxi_zone_lookup.csv (`nyc/ref`) | 12.0 K | 1 |

- Tổng dung lượng `nyc/raw` (`hdfs dfs -du -s`): 160389205 byte (152.96 MiB, 160.39 MB). Cột thứ hai (gồm replication) cũng là 160389205 vì replication = 1.

### 5.1b. Thời gian upload (ba lần, từ đường dẫn sạch)
- Lệnh (mỗi lần: `hdfs dfs -rm -r -f -skipTrash /user/tdat1/nyc/raw`, `mkdir -p`, rồi put): `time hdfs dfs -put ~/bda/data/raw/*.parquet /user/tdat1/nyc/raw/`
- Bằng chứng: `figures/data_04_upload_times.png`

| Lần | `real` | Throughput (160.39 MB / real) |
|---|---|---|
| 1 | 2.093 s | 76.6 MB/s |
| 2 | 2.254 s | 71.2 MB/s |
| 3 | 2.536 s | 63.2 MB/s |

- Trung bình thời gian: 2.294 s; nhanh nhất 2.093 s; chậm nhất 2.536 s. Throughput tính bằng tổng byte chia `real`, nên là throughput hiệu dụng của cả lệnh (gồm khởi động JVM), không phải tốc độ ghi thuần của HDFS.
- Ghi chú: nguồn nằm trên đĩa Linux, không phải `/mnt/c`; có thể có hiệu ứng bộ nhớ đệm của hệ điều hành giữa các lần chạy. Số này sẽ được dùng lại cho Bài 9 (cần bổ sung phép đo Spark `count`).

### 5.2. Trạng thái cluster sau khi nạp
- Trang NameNode (`figures/setup_08b_namenode_ui.png`): Live Nodes = 1, Dead Nodes = 0, Safemode off; 17 files and directories, 5 blocks; DFS Used = 154.21 MB (0.01%); Configured Capacity = 1006.85 GB; Started Thu Oct 01 01:13:29 +0700 2026.
- `bda-status.sh` (`figures/setup_11_bda_status.png`): NameNode, DataNode, ResourceManager, NodeManager, JobHistoryServer và sshd `UP`; Spark History Server, Kafka, MongoDB `DOWN` (chưa cài, không cần cho Lab 1); Live datanodes (1); Safe mode OFF.

### 5.3. Block và lưu trữ (§6)
- Lệnh: `hdfs fsck /user/tdat1/nyc/raw -files -blocks` (bằng chứng: `figures/hdfs_01_fsck_raw.png`, 25 dòng cuối của output).
- Kết quả: `The filesystem under path '/user/tdat1/nyc/raw' is HEALTHY`; Minimally replicated blocks: 3 (100.0 %); Over/Under/Mis-replicated blocks: 0; Default replication factor: 1; Average block replication: 1.0; Missing blocks: 0; Corrupt blocks: 0; Missing replicas: 0 (0.0 %).
- Số block: 3 block cho 3 file, tức mỗi file một block (suy ra từ "Minimally replicated blocks: 3" và 3 file trong `nyc/raw`; ảnh chỉ chụp phần cuối nên không thấy dòng liệt kê block của từng file). Giải thích: mỗi file nhỏ hơn block size 128 MB (47.6, 48.0, 57.3 MB) nên HDFS không cần cắt.
- Thứ tự thời gian: lệnh `fsck` kết thúc lúc Wed Sep 30 18:29:04 UTC 2026, trước ba lần nạp lại lúc khoảng 18:40 (UTC, theo `ls`). Dữ liệu nạp lại giống hệt nên kết luận về block vẫn áp dụng; ghi chú để thứ tự được nêu đúng.
- Thư mục lưu trữ (`figures/hdfs_02_nn_vs_dn.png`):
  - NameNode (`du -sh /opt/hadoop/data/nn`): 2.1M; chứa metadata nhỏ: `VERSION` (213 byte), các file `edits_*` (có file 1048576 byte).
  - DataNode (`du -sh /opt/hadoop/data/dn`): 155M; chứa các file `blk_1073741825` đến `blk_1073741829` (5 block, khớp "5 blocks" trên trang NameNode: 3 Parquet, 1 CSV, 1 `hello.txt`). Tên file gốc không xuất hiện trong thư mục này.
  - Tỷ lệ DataNode / NameNode: khoảng 73.8 lần (155 / 2.1; dùng giá trị `du -sh` đã làm tròn nên chỉ là ước lượng).
- Nhận định (bản nháp từ số đo, cần tự đọc lại và sửa cho đúng ý): thư mục NameNode nhỏ (2.1M) nhưng là phần duy nhất nối các block `blk_*` ẩn danh với tên file, nên tôi sẽ sao lưu thư mục NameNode thường xuyên hơn; thư mục DataNode lớn hơn khoảng 74 lần nên sao lưu tốn kém hơn và nhờ replication ở cluster thật mới được bảo vệ. Giới hạn: chỉ có 5 file, nên tỷ lệ này không ngoại suy được cho cluster lớn (kích thước NameNode phụ thuộc số file và số block, không phụ thuộc số byte).

### 5.4. MapReduce (§10) [chưa thực hiện]
- Map input / Map output / Reduce input / Reduce output records: [ĐIỀN]
- Tỷ lệ map output / reduce input: [ĐIỀN]

## 6. Sự cố và cách xử lý

| # | Sự cố | Nguyên nhân | Phát hiện và sửa | Bằng chứng |
|---|---|---|---|---|
| 1 | `syntax error near unexpected token 'newline'` ở dòng 446 của `hadoop-env.sh` khi chạy mọi lệnh Hadoop; dịch vụ vẫn khởi động | Nhập nguyên ký hiệu giữ chỗ `<tdat1>` (có ngoặc nhọn) cho `HDFS_NAMENODE_USER`; bash hiểu `<...>` là chuyển hướng | Đọc thông báo lỗi; sửa bằng `sed`; kiểm tra bằng `bash -n`; khởi động lại dịch vụ, không format lại | `figures/setup_08_jps_six_services.png`, `figures/setup_10_hdfs_hello_spark.png` (chụp trước khi sửa) |
| 2 | `-bash: /opt/spark/sbin: No such file or directory` khi nạp `.bashrc` | Dòng `export PATH=...` bị ngắt làm đôi khi nhập trong nano | Ghi lại khối cấu hình bằng `cat >> ~/.bashrc <<'EOF'` trên một dòng | `figures/setup_02_java_version.png` (dòng lỗi ở đầu ảnh) |
| 3 | `tên_bạn: No such file or directory` khi nạp `.bashrc` | Để nguyên `<tên_bạn>` trong `PYSPARK_PYTHON` | Thay bằng `/home/tdat1/...` | (không có ảnh) |
| 4 | `hdfs dfs` báo `Connection refused` ở `localhost:9000` | NameNode không chạy vì WSL dừng mọi dịch vụ khi đóng terminal cuối (§4.1) | `sudo service ssh start`, `bda-start.sh hdfs yarn jobhistory`, `hdfs dfsadmin -safemode wait`; không format lại | (không có ảnh) |
| 5 | `bda-status.sh` báo toàn bộ dịch vụ `DOWN` | Cùng nguyên nhân với sự cố 4 | Khởi động lại theo thứ tự trên | (không có ảnh) |
| 6 | `cp: target '.../lab01/src/': No such file or directory` | Thư mục `src` và `out` thiếu trong bản clone (Git không lưu thư mục rỗng) | `mkdir -p lab01/src lab01/out` rồi chép lại | (không có ảnh) |
| 7 | `git push` bị từ chối: `Password authentication is not supported` | GitHub không chấp nhận mật khẩu tài khoản | Xác thực bằng [ĐIỀN: `gh auth login` hay token] | (không có ảnh) |

## 7. Quyết định xử lý dữ liệu [chưa thực hiện, điền sau §11]

- Quy tắc curation áp dụng: [ĐIỀN]
- Số dòng giữ / loại / tỷ lệ loại (nêu mẫu số): [ĐIỀN]
- Quy tắc tôi sẽ đặt khác và lý do: [ĐIỀN]

## 8. Khai báo hỗ trợ AI (bản nháp, cần xác nhận)

Tôi dùng trợ lý AI (Claude) để: đọc và tóm tắt tài liệu Lab 1; hướng dẫn thứ tự các bước cài đặt; giải thích lỗi và đề xuất cách sửa (sự cố 1 đến 7); soạn mẫu cấu trúc repo; đọc ảnh minh chứng trong repo và soạn bản ghi này (kể cả bản nháp nhận định ở mục 5.3).
Cách tôi kiểm chứng: [ĐIỀN: ví dụ, tôi tự chạy từng lệnh trên máy mình, đối chiếu với Expected output trong tài liệu, và kiểm tra lại các số trong bản ghi với ảnh gốc].
Phần bài tập và nhận định: [ĐIỀN: tự viết / có tham khảo AI ở phần nào]. Mọi số liệu trong bản ghi này lấy từ ảnh và file do tôi tạo trên máy của mình.
