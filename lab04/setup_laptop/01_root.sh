#!/usr/bin/env bash
# Part 1 of the new-laptop setup: everything that needs root.
# Run from the tdat1 account:  sudo bash ~/Big_Data/lab04/setup_laptop/01_root.sh
# Installs Java 17, Python venv, jq, gh, MongoDB 8.0 + mongosh (manual Lab 4 section 2.1),
# Hadoop 3.4.3 into /opt/hadoop and Spark 3.5.9 into /opt/spark (the versions in setup/versions.txt).
# Safe to run again: steps already done are skipped.
set -euo pipefail
U=${SUDO_USER:-}
[ "$U" = tdat1 ] || { echo "STOP: run this with sudo from the Linux account tdat1 (HDFS user = Linux user). Now: '${U:-root}'"; exit 1; }
. /etc/os-release
echo "OS: $PRETTY_NAME"
[ "$VERSION_CODENAME" = noble ] || echo "WARNING: not Ubuntu 24.04 (noble); the MongoDB package line below is for noble"

HADOOP_V=3.4.3
SPARK_V=3.5.9

apt-get update
apt-get install -y openjdk-17-jdk-headless python3-venv python3-pip jq zip unzip curl wget git gh \
                   gnupg ca-certificates fonts-dejavu-core procps

# MongoDB 8.0, exactly as manual section 2.1
K=/usr/share/keyrings/mongodb-server-8.0.gpg
if ! command -v mongod >/dev/null; then
  curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | gpg -o $K --dearmor --yes
  echo "deb [ arch=amd64,arm64 signed-by=$K ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" \
    > /etc/apt/sources.list.d/mongodb-org-8.0.list
  apt-get update
  apt-get install -y mongodb-org
fi
mkdir -p /data/db /var/log/mongodb
chown -R "$U:$U" /data/db /var/log/mongodb

# Hadoop and Spark from the Apache archive, checked against the published SHA-512
fetch() {   # fetch <url> <dest dir name under /opt>
  local url=$1 dest=$2 f
  f=$(basename "$url")
  if [ -x "/opt/$dest/bin/$( [ "$dest" = hadoop ] && echo hdfs || echo spark-submit )" ]; then
    echo "/opt/$dest already installed, skipped"; return
  fi
  cd /tmp
  wget -q -O "$f" "$url"
  wget -q -O "$f.sha512" "$url.sha512"
  local want got
  want=$(tr -d ' \n\r\t' < "$f.sha512" | tr 'A-F' 'a-f' | grep -oE '[0-9a-f]{128}' | tail -1)
  got=$(sha512sum "$f" | cut -d' ' -f1)
  echo "$f sha512 published $want"
  echo "$f sha512 computed  $got"
  [ -n "$want" ] && [ "$want" = "$got" ] || { echo "STOP: checksum mismatch for $f"; exit 1; }
  tar -xzf "$f" -C /opt
  mv "/opt/${f%.t*gz}" "/opt/$dest"
  [ -d "/opt/$dest" ] || { echo "STOP: /opt/$dest not created"; exit 1; }
}
fetch "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_V/hadoop-$HADOOP_V.tar.gz" hadoop
fetch "https://archive.apache.org/dist/spark/spark-$SPARK_V/spark-$SPARK_V-bin-hadoop3.tgz" spark
mkdir -p /opt/hadoop/data
chown -R "$U:$U" /opt/hadoop /opt/spark

echo
echo "01_root.sh done. Next, WITHOUT sudo:  bash ~/Big_Data/lab04/setup_laptop/02_user.sh"
