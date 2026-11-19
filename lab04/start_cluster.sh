#!/usr/bin/env bash
# Start the cluster and MongoDB for a new WSL session (manual section 2.2).
# Run: bash ~/bda/lab04/start_cluster.sh
# Prints STOP and exits 1 if HDFS or MongoDB does not answer.
bda-start.sh
bda-status.sh
echo
if ! hdfs dfs -ls /user/tdat1/nyc >/dev/null; then
  echo "STOP: HDFS is not answering (Connection refused?). Do not run Spark jobs; report this output."
  exit 1
fi
echo "HDFS OK"
if ! mongosh --quiet --eval 'db.runCommand({ping: 1}).ok' 2>/dev/null | grep -q 1; then
  echo "STOP: MongoDB is not answering. Try 'bda-start.sh mongo' and 'tail -20 /var/log/mongodb/mongod.log'; report this output."
  exit 1
fi
echo "MongoDB OK"
