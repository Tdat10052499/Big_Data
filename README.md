# Big Data Analytics (72ITDS40303)

Kho lưu trữ bài thực hành môn Big Data Analytics, Văn Lang University.

## Cấu trúc

| Thư mục | Nội dung |
|---|---|
| `docs/` | Ghi chú tài liệu (không chứa file PDF của giảng viên) |
| `setup/` | Bản sao cấu hình Hadoop/Spark, snippet `.bashrc`, phiên bản phần mềm |
| `lab01/` | Lab 1: Data Quality and HDFS |

## Lab 1

```
lab01/
├── src/       # notebook và script (p2_nulls.py, p3_defects.py, ...)
├── runs/      # data_card.json, source_checksums.txt, run_record.md
├── out/       # quality_report.csv
├── figures/   # ảnh chụp minh chứng
└── answers.md # 12 Exercises kèm bằng chứng
```

Xem `lab01/README.md` để biết cách liên kết với đường dẫn `~/bda/lab01` mà tài liệu yêu cầu.

## Không commit

- Dữ liệu thô (`*.parquet`, `data/`): tải lại bằng lệnh `wget` trong tài liệu và đối chiếu `runs/source_checksums.txt`.
- Curated dataset: nằm trong HDFS tại `/user/<account>/nyc/curated`, không nộp trong archive.
- Môi trường ảo Python và khóa SSH.
