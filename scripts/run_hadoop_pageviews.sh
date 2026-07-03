#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${1:-/home/hadoop/single_project/hadoop-pageviews}"
JAR="${PROJECT_DIR}/target/hadoop-pageviews-1.0-SNAPSHOT.jar"
REDUCERS="${2:-4}"
INPUT="${3:-/user/hadoop/single_project/input/pageviews_24h}"
INTERMEDIATE="${4:-/user/hadoop/single_project/intermediate/pageviews_etl}"
OUTPUT="${5:-/user/hadoop/single_project/output/pageviews_analytics}"
TOP_N="${6:-20}"

hadoop jar "$JAR" it.unipi.solo.WikimediaPageviewAnalytics \
  "$REDUCERS" "$INPUT" "$INTERMEDIATE" "$OUTPUT" "$TOP_N"

hdfs dfs -ls "$OUTPUT"
