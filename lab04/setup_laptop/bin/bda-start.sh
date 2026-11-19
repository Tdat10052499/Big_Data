#!/usr/bin/env bash
# Start the services Lab 4 needs: HDFS (NameNode + DataNode) and MongoDB.
# Written for the new laptop; replaces the lecturer's bda-start.sh, which is not in the repo.
# Uses 'hdfs --daemon' (no ssh needed). MongoDB keeps its data in /data/db (manual section 2.2).
#   bda-start.sh          HDFS and MongoDB
#   bda-start.sh mongo    MongoDB only
set -u
start_hdfs() {
  jps | grep -qw NameNode || hdfs --daemon start namenode
  jps | grep -qw DataNode || hdfs --daemon start datanode
  hdfs dfsadmin -safemode wait >/dev/null 2>&1 || true
}
start_mongo() {
  if mongosh --quiet --eval 'db.runCommand({ping:1}).ok' >/dev/null 2>&1; then return; fi
  mongod --dbpath /data/db --logpath /var/log/mongodb/mongod.log --logappend \
         --bind_ip 127.0.0.1 --port 27017 --fork >/dev/null
}
case "${1:-all}" in
  mongo) start_mongo ;;
  hdfs)  start_hdfs ;;
  *)     start_hdfs; start_mongo ;;
esac
