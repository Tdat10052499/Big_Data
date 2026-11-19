# Lab 4 plan: Storage and Serving Architecture

Student: Hồ Du Tuấn Đạt, 2374802010097, HDFS user `tdat1`. Checkpoint Week 8 (10%).

## Deviations from the manual
1. Every HDFS path `/user/thaianh/...` is `/user/tdat1/...` (all scripts in `src/`, all commands in `prompts/`).
2. Every timing: 1 warm-up + 3 scored runs; all three runs and the best are recorded (`[timing]` lines printed by `best_of_three` in `src/l0_common.py`, the only change to the lecturer's logic).
3. Every percentage names its cohort; every number names its store and dataset (manual section 12).
4. Numbers in the manual (e.g. 356.5 M, 255 rows copied) are reference values only; each is reported as matching or differing from the measured value.
5. Exercises 5-8: the prediction is written by the student in `runs/exNN_prediction.txt` before measuring, and never edited afterwards.
6. Exercise 4 reader name: `2374802010097` (the student ID).

## Evidence layout
- Logs: `runs/<name>.log` (stdout, UTC timestamp first); Spark stderr in `runs/<name>.err`.
- Images: `figures/lab04_<name>.png`, rendered from the log by `src/log_to_png.py`.

## Steps
| Step | Manual | Script / command | Logs |
|---|---|---|---|
| P0 | 2.1-2.3, 3 | versions, inputs from Labs 1-3, sizes of Q1/Q2 data | `ex00_env`, `sec2_inputs` |
| P1 | 4.1-4.3 | `f1_formats.py --write`, `--plan`, `f2_footer.py` | `sec4_write`, `sec4_plan`, `sec4_footer` |
| Ex1 (7) | 12 | `f1_formats.py --write --month 2024-02`, then measure | `ex01_write`, `ex01_measure` |
| Ex2 (7) | 12 | footer of `agg/zone_hour` | `ex02_footer` |
| P2 | 5.1-5.4 | `d1_create.py`, `d2_merge.py`, `d3_history.py`, jq on the log | `sec5_*` |
| Ex3 (7) | 12 | `e3_zone_hour.py` -> `lake/zone_hour`, delete hour 3 | `ex03_zone_hour` |
| P3 | 10.1 | chmod / setfacl on `lake/zone_summary` as `reader` | `sec10_acl` |
| Ex4 (7) | 12 | same on `lake/zone_hour`, reader `2374802010097` | `ex04_acl` |
| P4 | 6, 7 | `d4_readers.py parquet|delta`, checkpoint vs `_delta_log` | `sec6_*`, `sec7_logs` |
| P5 | 8, 9 | `m1_load.py`, `m2_lookup.py`, `m3_trips.py`, `s1_compare.py` | `sec8_*`, `sec9_*` |
| P6 | 10.2-10.3 | commitInfo keys, trash interval, auth mode, small cells | `sec10_gov` |
| Ex5 (10) | 12 | prediction, then two new queries in `f1_formats.py` | `ex05_prediction.txt`, `ex05_measure` |
| Ex6 (10) | 12 | prediction, then 4 matched + 3 new corrections | `ex06_prediction.txt`, `ex06_merge` |
| Ex7 (10) | 12 | prediction, then 3 queries x 2 indexes on `trips_jan` | `ex07_prediction.txt`, `ex07_*` |
| Ex8 (10) | 12 | prediction, then `e8_append.py` x 4 | `ex08_prediction.txt`, `ex08_*` |
| Ex9 (16) | 12 | `e9_loadtest.py` -> `out/loadtest.csv` | `ex09_loadtest` |
| Ex10 (16) | 11-12 | `out/ADR-001.md` from the numbers above | - |
| Final | 13 | `runs/run_record.md`, `answers.md`, Word report, `make_zip.sh` | - |

Do not delete `/user/tdat1/nyc/{raw,ref,curated,curated_q1,agg,stream}`.
