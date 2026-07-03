#!/usr/bin/env bash
if [[ "$(whoami)" != "hadoop" ]]; then
  echo "ERROR: Run this script as the hadoop user. Use: su - hadoop"
  exit 1
fi
set -euo pipefail

BASE="/home/hadoop/single_project"
HDFS="/opt/hadoop/bin/hdfs"
LOCAL_DIR="$BASE/data/pageviews_16h"
HDFS_DIR="/user/hadoop/single_project/input/pageviews_16h"

mkdir -p "$LOCAL_DIR"

echo "Downloading missing 16h pageview files into $LOCAL_DIR"
"$BASE/scripts/download_pageviews.sh" 16 "$LOCAL_DIR"

echo "Refreshing HDFS input: $HDFS_DIR"
"$HDFS" dfs -rm -r -f "$HDFS_DIR" >/dev/null 2>&1 || true
"$HDFS" dfs -mkdir -p "$HDFS_DIR"
"$HDFS" dfs -put -f "$LOCAL_DIR"/pageviews-* "$HDFS_DIR/"

echo "Local size:"
du -sh "$LOCAL_DIR"

echo "HDFS size:"
"$HDFS" dfs -du -h "$HDFS_DIR"