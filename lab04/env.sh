# Source this at the start of every terminal: source ~/bda/lab04/env.sh
# It does what manual section 2.4 and 5.1 ask for (cd, venv, $SS, $T), with the tdat1 path.
cd ~/bda/lab04
source ~/bda/venv/bin/activate
SS="spark-submit --packages io.delta:delta-spark_2.12:3.2.1"
SS="$SS,org.mongodb.spark:mongo-spark-connector_2.12:10.4.0"
SS="$SS --py-files src/l0_common.py"
T=/user/tdat1/nyc/lake/zone_summary
export SS T
echo "lab04 env: $(pwd) | python $(which python) | T=$T"
