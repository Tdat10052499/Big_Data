# Lab 3 plan (Structured Streaming), one line per step with its logs

- S  Setup: branch, folders, lecturer files (SHA-256 checked), tdat1 edits, environment check, Kafka install — runs/lecturer_edits.diff, runs/s00_env.log, runs/s00_kafka_install.log, runs/s00_env_kafka.log, runs/bda_scripts_kafka.diff
- A  Sections 3-5: topic taxi-trips, producer 20,000 events, first stream — runs/sec3_topic.log, runs/sec4_producer.log, runs/sec5_first_stream.log
- B  Ex1 (taxi-ex1, offsets), Ex2 (dropoff zones), Ex3 (1 and 5 minute windows), Section 8 (watermark, taxi-late-demo), Ex4 (checkpoint) — runs/ex01_topic.log, runs/ex01_producer.log, runs/ex02_dropoff.log, runs/ex02_code.diff, runs/ex03_windows.log, runs/sec8_reset.log, runs/sec8_p1_producer.log, runs/sec8_phase1.log, runs/sec8_p2_producer.log, runs/sec8_phase2.log, runs/ex04_checkpoint.log
- C  Ex5 (prediction, then watermark 45 minutes, tag _wm45), Ex6 (prediction, then lateness of taxi-trips) — runs/ex05_prediction.txt, runs/ex05_reset.log, runs/ex05_p1_producer.log, runs/ex05_phase1.log, runs/ex05_p2_producer.log, runs/ex05_phase2.log, runs/ex06_prediction.txt, runs/ex06_lateness.log
- D  Section 10 (kill after 20 s, restart, taxi-restart), Ex7 (prediction, then kill after 5 s) — runs/sec10_{reset,producer,run1,kill,ck_after_kill,check_after_kill,run2,ck_after_restart,check_after_restart}.log, runs/ex07_prediction.txt, runs/ex07_*.log
- E  Section 11 (latency, taxi-live, 3 s), Ex8 (prediction, then triggers 1, 3, 10 s) — runs/sec11.log, runs/sec11_producer.log, runs/ex08_prediction.txt, runs/ex08_t{1,3,10}.log, runs/ex08_t{1,3,10}_producer.log, runs/ex08_summary.log
- F  Ex9 (rule fixed first, then rate sweep 1500..48000 at 3 s) — runs/ex09_rule.txt, runs/ex09_r<R>.log, runs/ex09_r<R>_producer.log, runs/ex09_r<R>_load.log, runs/ex09_summary.log
- G  Ex10 evidence (side effects repeated after a kill) — runs/ex10_run1.log, runs/ex10_ck_after_kill.log, runs/ex10_run2.log, runs/ex10_ck_after_restart.log, runs/ex10_side_effects.txt, runs/ex10_summary.log
- H  out/latency.csv and runs/run_record_facts.md — runs/make_latency.log, out/latency.csv, runs/run_record_facts.md
