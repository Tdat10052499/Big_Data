#!/usr/bin/env bash
# Stop HDFS and MongoDB. Written for the new laptop; replaces the lecturer's bda-stop.sh.
mongosh admin --quiet --eval 'db.shutdownServer()' >/dev/null 2>&1 || true
hdfs --daemon stop datanode 2>/dev/null || true
hdfs --daemon stop namenode 2>/dev/null || true
jps
