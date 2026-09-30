# Lab 1: Data Quality and HDFS

## Liên kết với đường dẫn của tài liệu

Script trong tài liệu dùng đường dẫn cố định `~/bda/lab01`. Clone repo rồi tạo symlink:

```bash
cd ~ && git clone https://github.com/Tdat10052499/Big_Data.git
mkdir -p ~/bda/data/raw
ln -s ~/Big_Data/lab01 ~/bda/lab01
cp /mnt/c/lab-files/src/*.py ~/Big_Data/lab01/src/
cp /mnt/c/lab-files/lab01_profile.ipynb ~/Big_Data/lab01/src/
```

`~/bda/venv` và `~/bda/data` nằm ngoài repo.

## Đóng gói nộp bài

```bash
cd ~/Big_Data/lab01
zip -r ~/lab01_<MÃ_SV>.zip src runs out answers.md figures
```

Không đưa curated dataset vào archive.
