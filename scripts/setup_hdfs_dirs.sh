#!/usr/bin/env bash
set -euo pipefail

/opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_8h
/opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_16h
/opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/single_project/input/pageviews_24h
/opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/single_project/intermediate
/opt/hadoop/bin/hdfs dfs -mkdir -p /user/hadoop/single_project/output
/opt/hadoop/bin/hdfs dfs -ls /user/hadoop/single_project
