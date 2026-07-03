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
YARN="/opt/hadoop/bin/yarn"

mkdir -p "$RESULT_DIR" "$RESULT_DIR/logs" || {
  echo "ERROR: Cannot create or write to $RESULT_DIR. Check permissions and ownership."
  ls -ld "$BASE/results" || true
  exit 1
}

CSV="$RESULT_DIR/memory_by_dataset.csv"
FAILED="$RESULT_DIR/failed_runs_memory_by_dataset.csv"
echo "dataset,hadoop_reducers,hadoop_avg_allocated_mb,hadoop_allocated_mb_seconds,sequential_peak_rss_mb" > "$CSV"
echo "benchmark,dataset,reducers,status,elapsed_seconds,log_file" > "$FAILED"

hadoop_allocated_mb_seconds() {
  local log_file="$1"
  local total=0
  local app

  for app in $(grep -o 'application_[0-9_]*' "$log_file" | sort -u); do
    local status_file="$RESULT_DIR/logs/${app}_status.log"
    "$YARN" application -status "$app" > "$status_file" 2>&1 || true
    local mb_seconds
    mb_seconds=$(sed -n 's/.*Aggregate Resource Allocation : \([0-9][0-9]*\) MB-seconds.*/\1/p' "$status_file" | tail -1)
    if [[ -n "${mb_seconds:-}" ]]; then
      total=$((total + mb_seconds))
    fi
  done

  echo "$total"
}

avg_mb() {
  local mb_seconds="$1"
  local seconds="$2"
  if [[ "$seconds" -le 0 || "$mb_seconds" -le 0 ]]; then
    echo "0.00"
    return
  fi
  awk -v mbs="$mb_seconds" -v sec="$seconds" 'BEGIN { printf "%.2f", mbs / sec }'
}

seq_peak_mb() {
  local time_file="$1"
  local kb
  kb=$(sed -n 's/.*Maximum resident set size (kbytes): \([0-9][0-9]*\).*/\1/p' "$time_file" | tail -1)
  if [[ -z "${kb:-}" ]]; then
    echo "0.00"
    return
  fi
  awk -v kb="$kb" 'BEGIN { printf "%.2f", kb / 1024 }'
}

run_case() {
  local label="$1"
  local hdfs_input="$2"
  local local_input="$3"
  local reducers=4
  local intermediate="/user/hadoop/single_project/intermediate/bench_memory_${label}_${reducers}r"
  local output="/user/hadoop/single_project/output/bench_memory_${label}_${reducers}r"
  local hadoop_log="$RESULT_DIR/logs/memory_${label}_hadoop.log"
  local seq_log="$RESULT_DIR/logs/memory_${label}_sequential.log"
  local seq_time_log="$RESULT_DIR/logs/memory_${label}_sequential_time.log"
  local seq_output="$RESULT_DIR/sequential_memory_${label}.json"

  echo "Running memory benchmark: $label"
  "$HDFS" dfs -rm -r -f "$intermediate" "$output" >/dev/null 2>&1 || true

  local start end elapsed allocated avg peak
  start=$(date +%s)
  "$RUN_HADOOP" "$JAR_DIR" "$reducers" "$hdfs_input" "$intermediate" "$output" 10 > "$hadoop_log" 2>&1
  local hadoop_code=$?
  end=$(date +%s)
  elapsed=$((end - start))

  if [[ "$hadoop_code" -ne 0 ]] || ! "$HDFS" dfs -test -e "$output/_SUCCESS"; then
    echo "memory_by_dataset,$label,$reducers,failed,$elapsed,$hadoop_log" >> "$FAILED"
    return
  fi

  allocated=$(hadoop_allocated_mb_seconds "$hadoop_log")
  avg=$(avg_mb "$allocated" "$elapsed")

  /usr/bin/time -v -o "$seq_time_log" "$RUN_SEQ" "$local_input" "$seq_output" 10 > "$seq_log" 2>&1
  local seq_code=$?
  if [[ "$seq_code" -ne 0 ]]; then
    echo "memory_by_dataset,$label,sequential,failed,0,$seq_log" >> "$FAILED"
    return
  fi
  peak=$(seq_peak_mb "$seq_time_log")

  echo "$label,$reducers,$avg,$allocated,$peak" >> "$CSV"
}

run_case "1h" "/user/hadoop/single_project/input/pageviews_1h" "$BASE/data/pageviews_8h/pageviews-20260501-000000.gz"
run_case "4h" "/user/hadoop/single_project/input/pageviews_4h" "$BASE/data/pageviews_4h"
run_case "8h" "/user/hadoop/single_project/input/pageviews_8h" "$BASE/data/pageviews_8h"
run_case "16h" "/user/hadoop/single_project/input/pageviews_16h" "$BASE/data/pageviews_16h"

echo "Completed. Successful results: $CSV"
echo "Failed runs, if any: $FAILED"
