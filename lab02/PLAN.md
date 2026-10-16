# Lab 2 plan: Spark Batch Processing

Student: Hồ Du Tuấn Đạt, 2374802010097, HDFS user `tdat1`. Checkpoint Week 4 (10%).

## Deviations from the manual
1. Every HDFS path `/user/thaianh/...` is `/user/tdat1/...` (all scripts in `src/`).
2. Every timing: 1 warm-up + 3 scored runs; all three runs and the best are recorded (`src/common.py: warm_then_three`).
3. Every percentage names its cohort, e.g. "of the N raw Jan+Feb rows".
4. Every speed claim cites the plan line that changed.
5. Numbers in the manual (e.g. 8,480,687) are reference values only; each is reported as matching or differing from the measured value.
6. Python 3.14: `toPandas()` fails (distutils); use `.collect()`.
7. Exercises 5-8: the prediction is written by the student in `runs/exNN_prediction.txt` before measuring, and never edited afterwards.

## Evidence layout
- Logs: `runs/<name>.log` (stdout, with a UTC timestamp at the top); stderr in `runs/<name>.err`.
- Screenshots: `figures/lab02_*.png`.

## Steps
| Step | Script | Evidence |
|---|---|---|
| Phase 0 | `start_cluster.sh`, `q1_scale.py` (Section 3) | `lab02_ex00_env.png`, `lab02_sec3_q1_scale.png`, `runs/sec3_q1_scale.log` |
| Ex1 (7) | `src/ex01_janfeb.py` -> `nyc/curated_jan_feb` | `runs/ex01_janfeb.log`, `lab02_ex01_janfeb.png` |
| Ex2 (7) | time lines a-e separately | `lab02_ex02_timings.png` |
| Ex3 (7) | `explain("formatted")`, payment_type / avg(tip_amount) | `lab02_ex03_plan.png` |
| Ex4 (7) | Spark UI :4040 (or `port_forward.py 4050 4040`) | `lab02_ex04_jobs.png`, `lab02_ex04_stage.png` |
| Ex5 (10) | shuffle partitions 2 vs 8, AQE off | `lab02_ex05_prediction.png`, `lab02_ex05_timings.png` |
| Ex6 (10) | AQE on/off on the Ex3 query, final plans | `lab02_ex06_prediction.png`, `_timings.png`, `_plan_aqe_off.png`, `_plan_aqe_on.png` |
| Ex7 (10) | join on DOLocationID, auto vs forced strategy | `lab02_ex07_prediction.png`, `_timings.png`, `_plans.png` |
| Ex8 (10) | cache vs no cache, 1-4 passes (cached includes fill) | `lab02_ex08_prediction.png`, `lab02_ex08_table.png` |
| Ex9 (16) | skew by PULocationID, task summary (AQE off, 8 partitions) | `lab02_ex09_numbers.png`, `lab02_ex09_task_summary.png` |
| Ex10 (16) | join + distance > 5 + avg fare by borough/hour, before/after | `lab02_ex10_timings.png`, `_plan_before.png`, `_plan_after.png` |
| Phase 4 | `q5_write.py` -> `nyc/agg`; run_record, answers, timings.csv, `make_zip.sh` | `lab02_sec10_agg.png` |

Do not delete `/user/tdat1/nyc/{raw,ref,curated,curated_q1,agg}`.
