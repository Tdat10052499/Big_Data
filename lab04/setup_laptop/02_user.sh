#!/usr/bin/env bash
# Part 2 of the new-laptop setup: everything that runs as tdat1, no sudo.
# Run:  bash ~/Big_Data/lab04/setup_laptop/02_user.sh
# Configures Hadoop (the XML files from setup/), Spark, the course venv ~/bda/venv,
# installs bda-start.sh / bda-status.sh / bda-stop.sh into ~/bin, formats HDFS once,
# starts HDFS and MongoDB and creates /user/tdat1/nyc.
set -euo pipefail
[ "$(whoami)" = tdat1 ] || { echo "STOP: run as the Linux user tdat1, not $(whoami)"; exit 1; }
REPO=~/Big_Data
L=$REPO/lab04/setup_laptop
[ -x /opt/hadoop/bin/hdfs ] && [ -x /opt/spark/bin/spark-submit ] || { echo "STOP: run 01_root.sh first"; exit 1; }

grep -q "Big Data Analytics lab" ~/.bashrc || { echo; cat "$L/bashrc_snippet.sh"; } >> ~/.bashrc
set +u; source "$L/bashrc_snippet.sh"; set -u

# Hadoop: the same four XML files as the Lab 1 machine (setup/ in the repo)
cp "$REPO"/setup/{core,hdfs,mapred,yarn}-site.xml "$HADOOP_CONF_DIR/"
grep -q "^export JAVA_HOME=" "$HADOOP_CONF_DIR/hadoop-env.sh" || cat >> "$HADOOP_CONF_DIR/hadoop-env.sh" <<'ENV'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HDFS_NAMENODE_USER=tdat1
export HDFS_DATANODE_USER=tdat1
export HDFS_SECONDARYNAMENODE_USER=tdat1
export YARN_RESOURCEMANAGER_USER=tdat1
export YARN_NODEMANAGER_USER=tdat1
ENV
# Spark
cat > "$SPARK_HOME/conf/spark-env.sh" <<'ENV'
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV

# Course Python
mkdir -p ~/bda/data/raw
[ -x ~/bda/venv/bin/python ] || python3 -m venv ~/bda/venv
~/bda/venv/bin/pip install -q --upgrade pip
~/bda/venv/bin/pip install -q pyarrow pandas pymongo pillow

# Start / status / stop scripts
mkdir -p ~/bin
cp "$L"/bin/bda-*.sh ~/bin/
chmod +x ~/bin/bda-*.sh

# Format HDFS only the first time
if [ ! -d /opt/hadoop/data/nn/current ]; then
  hdfs namenode -format -nonInteractive -force
fi
bda-start.sh
hdfs dfs -mkdir -p /user/tdat1/nyc
bda-status.sh
echo
echo "02_user.sh done. Open a NEW Ubuntu terminal (so ~/.bashrc is read) before starting Claude Code."
