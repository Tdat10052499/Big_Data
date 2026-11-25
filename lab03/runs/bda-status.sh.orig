#!/usr/bin/env bash
# Show which services are up. Written for the new laptop; replaces the lecturer's bda-status.sh.
G=$'\e[32m'; R=$'\e[31m'; N=$'\e[0m'
J=$(jps 2>/dev/null)
row() { if [ "$2" = 1 ]; then printf "  %-28s ${G}UP${N}\n" "$1"; else printf "  %-28s ${R}DOWN${N}\n" "$1"; fi; }
has() { echo "$J" | grep -qw "$1" && echo 1 || echo 0; }
row "HDFS NameNode"        "$(has NameNode)"
row "HDFS DataNode"        "$(has DataNode)"
row "YARN ResourceManager" "$(has ResourceManager)"
row "YARN NodeManager"     "$(has NodeManager)"
m=0; mongosh --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1 && m=1
row "MongoDB"              "$m"
echo
hdfs dfsadmin -report 2>/dev/null | grep -m1 "Live datanodes" | sed 's/^/  /'
hdfs dfsadmin -safemode get 2>/dev/null | sed 's/^/  /'
