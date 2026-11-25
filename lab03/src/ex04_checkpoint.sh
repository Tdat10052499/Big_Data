#!/usr/bin/env bash
# Exercise 4: inspect the Section 8 checkpoint of topic taxi-late-demo.
CK=hdfs://localhost:9000/user/tdat1/nyc/stream/ck/late_demo
H="hdfs dfs"
echo "\$ hdfs dfs -ls -R $CK"
$H -ls -R $CK
names() { $H -ls "$1" 2>/dev/null | awk '/^-/{n=split($NF,a,"/"); print a[n]}' | grep -E '^[0-9]+$' | sort -n; }
OFF=$(names $CK/offsets); COM=$(names $CK/commits)
N=$(echo "$OFF" | tail -1); M=$(echo "$COM" | tail -1)
echo
echo "# latest offsets file (highest batch number): $CK/offsets/$N"
echo "\$ hdfs dfs -cat $CK/offsets/$N"; $H -cat $CK/offsets/$N; echo
for b in $OFF; do
  [ "$b" = "$N" ] && continue
  echo "# other offsets file: $CK/offsets/$b"
  echo "\$ hdfs dfs -cat $CK/offsets/$b"; $H -cat $CK/offsets/$b; echo
done
echo "# commits: $(echo "$COM" | grep -c .) files, names: $(echo $COM)"
echo "# latest commits file: $CK/commits/$M"
echo "\$ hdfs dfs -cat $CK/commits/$M"; $H -cat $CK/commits/$M; echo
echo "# metadata"
echo "\$ hdfs dfs -cat $CK/metadata"; $H -cat $CK/metadata; echo
echo
echo "latest offsets batch = $N"
echo "latest commits batch = $M"
echo "last batch finished: $([ "$M" = "$N" ] && echo yes || echo no)"
echo
echo "# end offsets of topic taxi-late-demo (to compare with offsets/$N)"
bash "$(dirname "$0")/kafka_offsets.sh" taxi-late-demo
