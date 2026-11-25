#!/usr/bin/env bash
# Print the end offset of every partition of a topic, trying three GetOffsetShell variants
# (all attempts and their output are printed), then the sum of the end offsets.
#   bash src/kafka_offsets.sh <topic> [expected_sum]
# With --quiet only the first variant that works is shown (used inside polling loops).
T=$1; EXP=${2:-}; KB=/opt/kafka/bin; B=localhost:9092
out=""
try() {
  echo "\$ $*"
  r=$("$@" 2>&1); rc=$?
  echo "$r"; echo "exit $rc"
  if [ -z "$out" ] && [ $rc -eq 0 ] && echo "$r" | grep -qE "^$T:[0-9]+:[0-9]+$"; then out=$(echo "$r" | grep -E "^$T:[0-9]+:[0-9]+$"); fi
}
try $KB/kafka-run-class.sh kafka.tools.GetOffsetShell --bootstrap-server $B --topic "$T"
try $KB/kafka-run-class.sh org.apache.kafka.tools.GetOffsetShell --bootstrap-server $B --topic "$T"
try $KB/kafka-get-offsets.sh --bootstrap-server $B --topic "$T"
echo "end offsets of topic $T, per partition (from the first variant that worked):"
echo "$out" | sort -t: -k2,2n | awk -F: -v t="$T" '{printf "  topic %s partition %s end offset %s\n", t, $2, $3; s+=$3} END {printf "  sum of end offsets of topic %s: %d\n", t, s}'
if [ -n "$EXP" ]; then
  s=$(echo "$out" | awk -F: '{s+=$3} END {print s+0}')
  [ "$s" = "$EXP" ] && echo "  check: sum $s equals $EXP events sent to topic $T: PASS" || echo "  check: sum $s equals $EXP events sent to topic $T: FAIL"
fi
