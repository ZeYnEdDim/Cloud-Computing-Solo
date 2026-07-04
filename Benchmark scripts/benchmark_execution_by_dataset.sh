#!/usr/bin/env bash
if [[ "$(whoami)" != "hadoop" ]]; then
  echo "ERROR: Run this script as the hadoop user. Use: su - hadoop"
  exit 1
fi
set -u

BASE="/home/hadoop/single_project"
RESULT_DIR="$BASE/results/benchmark"
RUN_HADOOP="$BASE/scripts/run_hadoop_pageviews.sh"
RUN_SEQ="$BASE/scripts/run_sequential_pageviews.sh"
JAR_DIR="$BASE/hadoop-pageviews"
HDFS="/opt/hadoop/bin/hdfs"

mkdir -p "$RESULT_DIR/logs"

CSV="$RESULT_DIR/execution_time_by_dataset.csv"
FAILED="$RESULT_DIR/failed_runs_execution_by_dataset.csv"
echo "dataset,hadoop_reducers,hadoop_time_seconds,sequential_time_seconds" > "$CSV"
echo "benchmark,dataset,reducers,status,elapsed_seconds,log_file" > "$FAILED"

run_case() {
  local label="$1"
  local hdfs_input="$2"
  local local_input="$3"
  local reducers=4
  local intermediate="/user/hadoop/single_project/intermediate/bench_dataset_${label}_${reducers}r"
  local output="/user/hadoop/single_project/output/bench_dataset_${label}_${reducers}r"
  local hadoop_log="$RESULT_DIR/logs/execution_dataset_${label}_hadoop.log"
  local seq_log="$RESULT_DIR/logs/execution_dataset_${label}_sequential.log"
  local seq_output="$RESULT_DIR/sequential_dataset_${label}.json"

  echo "Running execution-time dataset benchmark: $label"
  "$HDFS" dfs -rm -r -f "$intermediate" "$output" >/dev/null 2>&1 || true

  local start end hadoop_time seq_time
  start=$(date +%s)
  "$RUN_HADOOP" "$JAR_DIR" "$reducers" "$hdfs_input" "$intermediate" "$output" 10 > "$hadoop_log" 2>&1
  local hadoop_code=$?
  end=$(date +%s)
  hadoop_time=$((end - start))

  if [[ "$hadoop_code" -ne 0 ]] || ! "$HDFS" dfs -test -e "$output/_SUCCESS"; then
    echo "execution_by_dataset,$label,$reducers,failed,$hadoop_time,$hadoop_log" >> "$FAILED"
    return
  fi

  start=$(date +%s)
  "$RUN_SEQ" "$local_input" "$seq_output" 10 > "$seq_log" 2>&1
  local seq_code=$?
  end=$(date +%s)
  seq_time=$((end - start))

  if [[ "$seq_code" -ne 0 ]]; then
    echo "execution_by_dataset,$label,sequential,failed,$seq_time,$seq_log" >> "$FAILED"
    return
  fi

  echo "$label,$reducers,$hadoop_time,$seq_time" >> "$CSV"
}

run_case "1h" "/user/hadoop/single_project/input/pageviews_1h" "$BASE/data/pageviews_8h/pageviews-20260501-000000.gz"
run_case "4h" "/user/hadoop/single_project/input/pageviews_4h" "$BASE/data/pageviews_4h"
run_case "8h" "/user/hadoop/single_project/input/pageviews_8h" "$BASE/data/pageviews_8h"
run_case "16h" "/user/hadoop/single_project/input/pageviews_16h" "$BASE/data/pageviews_16h"

echo "Completed. Successful results: $CSV"
echo "Failed runs, if any: $FAILED"
