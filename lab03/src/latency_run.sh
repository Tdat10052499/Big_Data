#!/usr/bin/env bash
# Lab 3, Section 11 / Exercises 8-9: one latency run on topic taxi-live.
#   bash src/latency_run.sh <tag> <trigger_s> <rate> <limit>
#   LOADLOG=1 bash src/latency_run.sh ...   also logs uptime and free -m every 5 s to runs/<tag>_load.log
# Logs: runs/<tag>.log (+ .err) for Spark, runs/<tag>_producer.log for the producer.
set -u
TAG=$1; TRIG=$2; RATE=$3; LIMIT=$4; TOPIC=taxi-live
cd ~/bda/lab03
SS="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 --py-files src/s0_common.py"
KT=/opt/kafka/bin/kafka-topics.sh
pgrep -f trip_producer.py >/dev/null && { echo "STOP: a trip_producer.py is still running"; exit 1; }
if ! $KT --bootstrap-server localhost:9092 --list | grep -qx $TOPIC; then
  echo "creating topic $TOPIC (3 partitions, replication factor 1)"
  $KT --bootstrap-server localhost:9092 --create --topic $TOPIC --partitions 3 --replication-factor 1
fi
if [ "${LOADLOG:-0}" = 1 ]; then
  ( date -u; while true; do echo "--- $(date -u +'%Y-%m-%d %H:%M:%S') UTC"; uptime; free -m; sleep 5; done ) > runs/${TAG}_load.log 2>&1 &
  LOADPID=$!
fi
( { date -u; $SS src/s4_latency.py --trigger $TRIG --batches 8 --tag $TAG --topic $TOPIC 2>runs/${TAG}.err; echo "spark-submit exit $?"; } > runs/${TAG}.log ) &
SPARK=$!
for _ in $(seq 1 180); do grep -q "watching" runs/${TAG}.log && break; kill -0 $SPARK 2>/dev/null || break; sleep 1; done
grep -q "watching" runs/${TAG}.log || { echo "STOP: no 'watching' line"; cat runs/${TAG}.log; tail -40 runs/${TAG}.err; [ -n "${LOADPID:-}" ] && kill $LOADPID; exit 1; }
echo "$(date -u +'%H:%M:%S') watching line seen; starting producer at $RATE events/s, limit $LIMIT"
( { date -u; ~/bda/venv/bin/python src/trip_producer.py --topic $TOPIC --rate $RATE --limit $LIMIT --late-fraction 0; } > runs/${TAG}_producer.log 2>&1 ) &
PROD=$!
for _ in $(seq 1 600); do kill -0 $SPARK 2>/dev/null || break; sleep 1; done
if kill -0 $SPARK 2>/dev/null; then echo "WARNING: Spark run $TAG still running after 600 s"; fi
wait $SPARK 2>/dev/null
echo "$(date -u +'%H:%M:%S') Spark run exited; stopping producer"
pkill -INT -f trip_producer.py
wait $PROD 2>/dev/null
[ -n "${LOADPID:-}" ] && { kill $LOADPID; wait $LOADPID 2>/dev/null; }
echo "--- runs/${TAG}.log"; cat runs/${TAG}.log
echo "--- runs/${TAG}_producer.log"; cat runs/${TAG}_producer.log
