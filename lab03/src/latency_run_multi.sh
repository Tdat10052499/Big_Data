#!/usr/bin/env bash
# Lab 3, Exercise 9 supplement Part A: one latency run on taxi-live with k producers in parallel.
#   bash src/latency_run_multi.sh <tag> <k> <rate_each> <limit_each>
# Logs: runs/<tag>.log (+ .err) Spark; runs/<tag>_producer<i>.log, i = 1..k (seed 41+i);
#       runs/<tag>_load.log every 5 s: uptime, free -m, top -b -n 1 -o %CPU | head -15
set -u
TAG=$1; K=$2; RATE=$3; LIMIT=$4; TOPIC=taxi-live
cd ~/bda/lab03
SS="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 --py-files src/s0_common.py"
KT=/opt/kafka/bin/kafka-topics.sh
pgrep -f trip_producer.py >/dev/null && { echo "STOP: a trip_producer.py is still running"; exit 1; }
if ! $KT --bootstrap-server localhost:9092 --list | grep -qx $TOPIC; then
  $KT --bootstrap-server localhost:9092 --create --topic $TOPIC --partitions 3 --replication-factor 1
fi
( date -u; while true; do echo "--- $(date -u +'%Y-%m-%d %H:%M:%S') UTC"; uptime; free -m; top -b -n 1 -o %CPU | head -15; sleep 5; done ) > runs/${TAG}_load.log 2>&1 &
LOADPID=$!
( { date -u; $SS src/s4_latency.py --trigger 3 --batches 8 --tag $TAG --topic $TOPIC 2>runs/${TAG}.err; echo "spark-submit exit $?"; } > runs/${TAG}.log ) &
SPARK=$!
for _ in $(seq 1 180); do grep -q "watching" runs/${TAG}.log && break; kill -0 $SPARK 2>/dev/null || break; sleep 1; done
grep -q "watching" runs/${TAG}.log || { echo "STOP: no 'watching' line"; cat runs/${TAG}.log; tail -40 runs/${TAG}.err; kill $LOADPID; exit 1; }
echo "$(date -u +'%H:%M:%S') watching line seen; starting $K producers at $RATE events/s each, limit $LIMIT each"
PRODS=()
for i in $(seq 1 $K); do
  ( { date -u; ~/bda/venv/bin/python src/trip_producer.py --topic $TOPIC --rate $RATE --limit $LIMIT --late-fraction 0 --seed $((41 + i)); } > runs/${TAG}_producer$i.log 2>&1 ) &
  PRODS+=($!)
done
for _ in $(seq 1 600); do kill -0 $SPARK 2>/dev/null || break; sleep 1; done
kill -0 $SPARK 2>/dev/null && echo "WARNING: Spark run $TAG still running after 600 s"
wait $SPARK 2>/dev/null
echo "$(date -u +'%H:%M:%S') Spark run exited; stopping producers"
pkill -INT -f trip_producer.py
for p in "${PRODS[@]}"; do wait $p 2>/dev/null; done
kill $LOADPID; wait $LOADPID 2>/dev/null
for i in $(seq 1 $K); do grep -q "run summary" runs/${TAG}_producer$i.log || echo "WARNING: no run summary in ${TAG}_producer$i.log"; done
echo "--- runs/${TAG}.log"; cat runs/${TAG}.log
for i in $(seq 1 $K); do echo "--- runs/${TAG}_producer$i.log (summary)"; sed -n '/run summary/,$p' runs/${TAG}_producer$i.log; done
