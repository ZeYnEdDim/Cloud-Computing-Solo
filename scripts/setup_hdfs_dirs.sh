#!/usr/bin/env bash
set -euo pipefail

hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_8h
hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_16h
hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_24h
hdfs dfs -mkdir -p /user/hadoop/single_project/intermediate
hdfs dfs -mkdir -p /user/hadoop/single_project/output
hdfs dfs -ls /user/hadoop/single_project
