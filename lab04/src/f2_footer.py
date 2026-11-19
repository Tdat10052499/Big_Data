"""Lab 4, Section 4: open one Parquet file and print what its footer says.

Runs with plain Python, not Spark. It needs pyarrow, which is in ~/bda/venv.

    hdfs dfs -get <one part file> out/sample.parquet
    python src/f2_footer.py out/sample.parquet
    python src/f2_footer.py out/sample.parquet trip_distance fare_amount
"""
import sys

import pyarrow.parquet as pq

path = sys.argv[1]
wanted = sys.argv[2:] or ["tpep_pickup_datetime", "trip_distance", "fare_amount", "pickup_date"]

f = pq.ParquetFile(path)
meta = f.metadata
print()
print(f"  file          {path}")
print(f"  rows          {meta.num_rows:,}")
print(f"  row groups    {meta.num_row_groups}")
print(f"  columns       {meta.num_columns}")
print(f"  written by    {meta.created_by.split(' (')[0]}")
print()

for g in range(meta.num_row_groups):
    rg = meta.row_group(g)
    print(f"  row group {g}: {rg.num_rows:,} rows, {rg.total_byte_size / 1048576:,.1f} MB before compression")
    print(f"    {'column':24}{'codec':>8}{'stored':>11}   {'min':>20}   {'max':>20}")
    for i in range(rg.num_columns):
        c = rg.column(i)
        if c.path_in_schema not in wanted:
            continue
        s = c.statistics
        lo = str(s.min)[:20] if s is not None and s.has_min_max else "-"
        hi = str(s.max)[:20] if s is not None and s.has_min_max else "-"
        print(f"    {c.path_in_schema:24}{c.compression:>8}{c.total_compressed_size / 1024:>8,.0f} KB"
              f"   {lo:>20}   {hi:>20}")
    print()
