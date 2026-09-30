# Lab 1: Answers

> Mọi con số phải có bằng chứng (ảnh trong `figures/`, hoặc số trong `runs/run_record.md`).
> Mọi phần trăm phải nêu mẫu số (cohort).

## Exercise 1: Load February and report its blocks (5đ)
- Dự đoán / kết quả:
- Kích thước file:
- Số block:
- Block size:
- Replication:
- Giải thích (1 câu):
- Evidence: `figures/`

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
- Dự đoán (ghi TRƯỚC khi chạy, kèm thời điểm):
- Kết quả đo (fsck):
- Khớp hay không, giải thích:
- Evidence:

## Exercise 6: The cost of the wrong curation rule (8đ)
- Số dòng mới / chênh lệch so với 2,724,143:
- Mean fare_amount (quy tắc gốc / quy tắc mới):
- Ngưỡng "đáng kể" (định nghĩa trước) và so với cái gì:
- Evidence:

## Exercise 7: Find a defect this manual did not list (8đ)
- Kiểm tra (code):
- Số dòng và tỷ lệ (nêu cohort):
- Vì sao là lỗi chứ không chỉ bất thường (2 câu):

## Exercise 8: Kill a service and read the failure (8đ)
- Trước / trong / sau (screenshot):
- Lỗi chính xác của `hdfs dfs -cat`:
- Live nodes theo `dfsadmin -report`:
- Giải thích (liên hệ §6):

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
