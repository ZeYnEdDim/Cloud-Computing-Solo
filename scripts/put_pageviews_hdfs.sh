#!/usr/bin/env bash
set -euo pipefail

LOCAL_DIR="${1:-/home/hadoop/single_project/data/pageviews_24h}"
HDFS_DIR="${2:-/user/hadoop/single_project/input/pageviews_24h}"

hdfs dfs -mkdir -p "$HDFS_DIR"
hdfs dfs -put -f "${LOCAL_DIR}"/*.gz "$HDFS_DIR"/
hdfs dfs -ls -h "$HDFS_DIR"
