#!/usr/bin/env bash
# Start the Lab 1 cluster for a new WSL session and check that HDFS answers.
# Run: bash ~/bda/lab02/start_cluster.sh
# Exits with status 1 (and prints STOP) if HDFS still refuses connections.
bda-start.sh
bda-status.sh
echo
if hdfs dfs -ls /user/tdat1/nyc/raw; then
  echo "HDFS OK"
else
  echo "STOP: HDFS is not answering (Connection refused?). Do not run Spark jobs; report this output."
  exit 1
fi
