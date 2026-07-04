#!/usr/bin/env bash
if [[ "$(whoami)" != "hadoop" ]]; then
  echo "ERROR: Run this script as the hadoop user. Use: su - hadoop"
  exit 1
fi
set -u

BASE="/home/hadoop/single_project"
RESULT_DIR="$BASE/results/benchmark"
RUN_HADOOP="$BASE/scripts/run_hadoop_pageviews.sh"
JAR_DIR="$BASE/hadoop-pageviews"
HDFS="/opt/hadoop/bin/hdfs"

mkdir -p "$RESULT_DIR" "$RESULT_DIR/logs" || {
  echo "ERROR: Cannot create or write to $RESULT_DIR. Check permissions and ownership."
  ls -ld "$BASE/results" || true
  exit 1
}

CSV="$RESULT_DIR/execution_time_by_reducers.csv"
FAILED="$RESULT_DIR/failed_runs_execution_by_reducers.csv"
echo "dataset,reducers,hadoop_time_seconds" > "$CSV"
echo "benchmark,dataset,reducers,status,elapsed_seconds,log_file" > "$FAILED"

run_case() {
  local reducers="$1"
  local label="4h"
  local intermediate="/user/hadoop/single_project/intermediate/bench_reducers_${label}_${reducers}r"
  local output="/user/hadoop/single_project/output/bench_reducers_${label}_${reducers}r"
  local log_file="$RESULT_DIR/logs/execution_reducers_${label}_${reducers}r.log"

  echo "Running reducer benchmark: $label with $reducers reducers"
  "$HDFS" dfs -rm -r -f "$intermediate" "$output" >/dev/null 2>&1 || true

  local start end elapsed
  start=$(date +%s)
  "$RUN_HADOOP" "$JAR_DIR" "$reducers" "/user/hadoop/single_project/input/pageviews_4h" "$intermediate" "$output" 10 > "$log_file" 2>&1
  local code=$?
  end=$(date +%s)
  elapsed=$((end - start))

  if [[ "$code" -ne 0 ]] || ! "$HDFS" dfs -test -e "$output/_SUCCESS"; then
    echo "execution_by_reducers,$label,$reducers,failed,$elapsed,$log_file" >> "$FAILED"
    return
  fi

  echo "$label,$reducers,$elapsed" >> "$CSV"
}

run_case 1
run_case 2
run_case 4
run_case 8

echo "Completed. Successful results: $CSV"
echo "Failed runs, if any: $FAILED"
