#!/usr/bin/env bash
# Lab 3, Section 10 / Exercise 7: kill the restart query part way through, inspect, restart, inspect.
#   bash src/kill_restart.sh <tag> <seconds>
# Every step logs to runs/<tag>_*.log (stdout, date -u first line) and Spark stderr to runs/<tag>_*.err.
set -u
TAG=$1; SEC=$2
cd ~/bda/lab03
SS="spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9 --py-files src/s0_common.py"
STREAM=hdfs://localhost:9000/user/tdat1/nyc/stream
CK=$STREAM/ck/restart; OUT=$STREAM/restart; TOPIC=taxi-restart
TMP=$(mktemp -d)
utc() { date -u +"%Y-%m-%d %H:%M:%S.%3N"; }
names() { hdfs dfs -ls "$1" 2>/dev/null | awk '/^-/{n=split($NF,a,"/"); print a[n]}' | grep -E '^[0-9]+$' | sort -n; }

inspect() {   # $1 = label
  echo "# checkpoint inspection $1"
  for d in $CK/offsets $CK/commits $OUT/_spark_metadata; do echo "\$ hdfs dfs -ls $d"; hdfs dfs -ls $d; done
  N=$(names $CK/offsets | tail -1); M=$(names $CK/commits | tail -1)
  echo "# latest offsets file: $CK/offsets/$N"
  echo "\$ hdfs dfs -cat $CK/offsets/$N"; hdfs dfs -cat $CK/offsets/$N; echo
  echo "latest offsets batch N = $N"
  echo "latest commits batch M = $M"
  echo "interrupted batch present: $([ "${N:--1}" -gt "${M:--1}" ] && echo yes || echo no)"
  echo "part-*.parquet files in $OUT (output of topic $TOPIC): $(hdfs dfs -ls $OUT | grep -cE '/part-[^/]*\.parquet$')"
  tot=0
  for f in $(hdfs dfs -ls $OUT/_spark_metadata | awk '/^-/{print $NF}'); do
    c=$(hdfs dfs -cat "$f" | grep -o '"path"' | wc -l); tot=$((tot + c))
    echo "  _spark_metadata file ${f##*/}: $c \"path\" entries"
  done
  echo "files listed in all _spark_metadata log files (\"path\" entries, summed over the files above): $tot"
  echo "# end offsets of topic $TOPIC"
  bash src/kafka_offsets.sh $TOPIC
}

# 1. reset
{ date -u; $SS src/s3_restart.py --reset 2>runs/${TAG}_reset.err; echo "spark-submit exit $?"; } | tee runs/${TAG}_reset.log
# 2. producer, wait for it
{ date -u; ~/bda/venv/bin/python src/trip_producer.py --topic $TOPIC --limit 40000 --rate 6000 --late-fraction 0; } 2>&1 | tee runs/${TAG}_producer.log
# 3. run 1 in the background, kill -9 after SEC seconds
( { date -u; $SS src/s3_restart.py 2>runs/${TAG}_run1.err; echo "spark-submit exit $?"; } > runs/${TAG}_run1.log ) &
RUN1=$!
for _ in $(seq 1 180); do grep -q "query started at" runs/${TAG}_run1.log && break; sleep 1; done
grep -q "query started at" runs/${TAG}_run1.log || { echo "STOP: no 'query started at' line within 180 s"; cat runs/${TAG}_run1.log; tail -40 runs/${TAG}_run1.err; exit 1; }
seen=$(utc)
sleep $SEC
killed=$(utc)
{ date -u
  start=$(grep -m1 "query started at" runs/${TAG}_run1.log | sed -E 's/.*query started at (.*) UTC.*/\1/')
  echo "query started at (from ${TAG}_run1.log): $start UTC"
  echo "start line seen by kill_restart.sh at:    $seen UTC"
  echo "sleep $SEC"
  echo "\$ pkill -9 -e -f s3_restart.py   at $killed UTC"
  pkill -9 -e -f s3_restart.py
  echo "seconds between the start line and the kill: $(python3 -c "import datetime as d,sys;f='%Y-%m-%d %H:%M:%S.%f';print(f'{(d.datetime.strptime(sys.argv[2],f)-d.datetime.strptime(sys.argv[1],f)).total_seconds():.3f}')" "$start" "$killed")"
} 2>&1 | tee runs/${TAG}_kill.log
wait $RUN1 2>/dev/null
echo "--- runs/${TAG}_run1.log"; cat runs/${TAG}_run1.log
# 4. inspection after the kill
{ date -u; inspect "after the kill"; } 2>&1 | tee runs/${TAG}_ck_after_kill.log
names $CK/commits > $TMP/commits_after_kill
# 5. check after the kill
{ date -u; $SS src/s3_restart.py --check 2>runs/${TAG}_check_after_kill.err; echo "spark-submit exit $?"; } | tee runs/${TAG}_check_after_kill.log
# 6. restart in the background, poll until caught up, then Ctrl+C (SIGINT)
( { date -u; $SS src/s3_restart.py 2>runs/${TAG}_run2.err; echo "spark-submit exit $?"; } > runs/${TAG}_run2.log ) &
RUN2=$!
{ date -u
  echo "# polling every 5 s (max 300 s): latest commits batch == latest offsets batch AND latest offsets == end offsets of topic $TOPIC"
  done_ok=no
  for i in $(seq 1 60); do
    sleep 5
    N=$(names $CK/offsets | tail -1); M=$(names $CK/commits | tail -1)
    ck=$(hdfs dfs -cat $CK/offsets/$N 2>/dev/null | tail -1)
    end=$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic $TOPIC 2>/dev/null)
    eq=$(python3 -c "
import json,sys
ck=json.loads(sys.argv[1]).get('$TOPIC',{}) if sys.argv[1].startswith('{') else {}
end={l.split(':')[1]:int(l.split(':')[2]) for l in sys.argv[2].split() if l.count(':')==2}
print('yes' if ck and {k:int(v) for k,v in ck.items()}==end else 'no')" "$ck" "$end")
    echo "$(utc) UTC poll $i: offsets batch $N, commits batch $M, offsets $ck, topic $TOPIC end $(echo $end | tr '\n' ' '), offsets equal end: $eq"
    if [ "$N" = "$M" ] && [ "$eq" = yes ]; then done_ok=yes; break; fi
  done
  echo "caught up: $done_ok"
  echo "\$ pkill -INT -e -f s3_restart.py   at $(utc) UTC"
  pkill -INT -e -f s3_restart.py
} 2>&1 | tee runs/${TAG}_restart_wait.log
for _ in $(seq 1 120); do kill -0 $RUN2 2>/dev/null || break; sleep 1; done
kill -0 $RUN2 2>/dev/null && echo "WARNING: run 2 still running 120 s after SIGINT" | tee -a runs/${TAG}_restart_wait.log
wait $RUN2 2>/dev/null
echo "--- runs/${TAG}_run2.log"; cat runs/${TAG}_run2.log
# 7. inspection after the restart
{ date -u; inspect "after the restart"
  echo "batch ids committed after the restart: $(comm -13 <(sort $TMP/commits_after_kill) <(names $CK/commits | sort) | sort -n | tr '\n' ' ')"
} 2>&1 | tee runs/${TAG}_ck_after_restart.log
# 8. check after the restart
{ date -u; $SS src/s3_restart.py --check 2>runs/${TAG}_check_after_restart.err; echo "spark-submit exit $?"; } | tee runs/${TAG}_check_after_restart.log
rm -rf $TMP
