#!/usr/bin/env bash
# Lab 3, Exercise 10 evidence driver. Logs runs/ex10_*.log.
set -u
cd ~/bda/lab03
SS="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 --py-files src/s0_common.py"
CK=hdfs://localhost:9000/user/tdat1/nyc/stream/ck/ex10_repeat
SIDE=runs/ex10_side_effects.txt
utc() { date -u +"%Y-%m-%d %H:%M:%S.%3N"; }
names() { hdfs dfs -ls "$1" 2>/dev/null | awk '/^-/{n=split($NF,a,"/"); print a[n]}' | grep -E '^[0-9]+$' | sort -n; }
dump() {
  for d in $CK/offsets $CK/commits; do echo "\$ hdfs dfs -ls $d"; hdfs dfs -ls $d; done
  N=$(names $CK/offsets | tail -1); M=$(names $CK/commits | tail -1)
  echo "\$ hdfs dfs -cat $CK/offsets/$N"; hdfs dfs -cat $CK/offsets/$N; echo
  echo "latest offsets batch N = $N"; echo "latest commits batch M = $M"
  echo "interrupted batch present: $([ "${N:--1}" -gt "${M:--1}" ] && echo yes || echo no)"
  echo "side-effect lines in $SIDE: $(wc -l < $SIDE)"
}
lines() { wc -l < $SIDE; }
{ date -u; $SS src/ex10_repeat.py --reset 2>runs/ex10_reset.err; echo "spark-submit exit $?"; } | tee runs/ex10_reset.log
( { date -u; $SS src/ex10_repeat.py 2>runs/ex10_run1.err; echo "spark-submit exit $?"; } > runs/ex10_run1.log ) &
R1=$!
for _ in $(seq 1 300); do [ "$(lines)" -ge 4 ] && break; sleep 0.2; done
{ date -u; echo "$(utc) UTC 4th side-effect line seen; sleep 1"; } | tee runs/ex10_kill.log
sleep 1
echo "\$ pkill -9 -e -f ex10_repeat.py   at $(utc) UTC" | tee -a runs/ex10_kill.log
pkill -9 -e -f ex10_repeat.py 2>&1 | tee -a runs/ex10_kill.log
wait $R1 2>/dev/null
echo "--- runs/ex10_run1.log"; cat runs/ex10_run1.log
{ date -u; echo "# checkpoint after the kill"; dump; } 2>&1 | tee runs/ex10_ck_after_kill.log
BASE=$(lines)
( { date -u; $SS src/ex10_repeat.py 2>runs/ex10_run2.err; echo "spark-submit exit $?"; } > runs/ex10_run2.log ) &
R2=$!
for _ in $(seq 1 1500); do [ "$(lines)" -ge $((BASE + 3)) ] && break; sleep 0.2; done
echo "$(utc) UTC 3 more side-effect lines seen ($(lines) in total)" | tee -a runs/ex10_kill.log
echo "\$ pkill -INT -e -f ex10_repeat.py   at $(utc) UTC" | tee -a runs/ex10_kill.log
pkill -INT -e -f ex10_repeat.py 2>&1 | tee -a runs/ex10_kill.log
for _ in $(seq 1 120); do kill -0 $R2 2>/dev/null || break; sleep 1; done
wait $R2 2>/dev/null
echo "--- runs/ex10_run2.log"; cat runs/ex10_run2.log
{ date -u; echo "# checkpoint after the restart"; dump; } 2>&1 | tee runs/ex10_ck_after_restart.log
{ date -u; ~/bda/venv/bin/python src/ex10_summary.py; } 2>&1 | tee runs/ex10_summary.log
