# Cloud Computing Solo Project - Wikimedia Pageview Analytics

This project is the single-student version of the Cloud Computing final project. It implements a Hadoop MapReduce analytics workflow over Wikimedia hourly pageview dumps.

## Dataset

Dataset source:

https://dumps.wikimedia.org/other/pageviews/2026/2026-05/

Selected data:

- Day: 2026-05-01
- Files: `pageviews-20260501-000000.gz` to `pageviews-20260501-230000.gz`
- Full input size target: around 1-1.5 GB compressed
- Benchmark subsets:
  - 8 hourly files
  - 16 hourly files
  - 24 hourly files

Each input line has the following format:

```text
project_code page_title view_count total_response_size
```

Example:

```text
en Main_Page 42 50043
```

## Analytics Workflow

The Hadoop implementation is composed of two non-iterative MapReduce jobs in cascade.

### Job 1 - ETL and Classification

The first job parses raw Wikimedia pageview records and emits structured `PageviewRecord` objects using a custom Hadoop `Writable`.

It performs:

- invalid line filtering;
- numeric parsing of view count and response size;
- extraction of the hour from the input filename;
- classification of access type as `mobile` or `desktop`;
- serialization into a SequenceFile intermediate dataset.

### Job 2 - Aggregation and Ranking

The second job performs distributed analytics using in-mapper combining and reducers.

It computes:

- total views;
- total response bytes;
- top projects by views;
- top pages by views;
- views by hour;
- mobile vs desktop traffic;
- top projects by response bytes.

Advanced Hadoop features used:

- two MapReduce jobs in cascade;
- custom `Writable`;
- in-mapper combining;
- custom partitioner;
- `setup()` and `cleanup()` methods;
- multiple reducers.

## Local Project Structure

```text
hadoop-pageviews/
  pom.xml
  src/main/java/it/unipi/solo/WikimediaPageviewAnalytics.java
sequential/
  pageview_sequential.py
scripts/
  download_pageviews.sh
  put_pageviews_hdfs.sh
  run_hadoop_pageviews.sh
  run_sequential_pageviews.sh
results/
```

## VM Project Structure

On the VM, use a separate folder so this project does not conflict with the previous group project:

```bash
/home/hadoop/single_project
```

Recommended HDFS paths:

```bash
/user/hadoop/single_project/input/pageviews_8h
/user/hadoop/single_project/input/pageviews_16h
/user/hadoop/single_project/input/pageviews_24h
/user/hadoop/single_project/intermediate/pageviews_etl
/user/hadoop/single_project/output/pageviews_analytics
```

## Setup on VM

Copy this project to the VM under:

```bash
/home/hadoop/single_project
```

Make scripts executable:

```bash
chmod +x /home/hadoop/single_project/scripts/*.sh
```

Build the Hadoop project:

```bash
cd /home/hadoop/single_project/hadoop-pageviews
mvn clean package
```

## Download Dataset

Download 24 hourly files:

```bash
/home/hadoop/single_project/scripts/download_pageviews.sh 24 /home/hadoop/single_project/data/pageviews_24h
```

For smaller benchmark subsets:

```bash
/home/hadoop/single_project/scripts/download_pageviews.sh 8 /home/hadoop/single_project/data/pageviews_8h
/home/hadoop/single_project/scripts/download_pageviews.sh 16 /home/hadoop/single_project/data/pageviews_16h
```

## Upload Dataset to HDFS

```bash
/home/hadoop/single_project/scripts/put_pageviews_hdfs.sh \
  /home/hadoop/single_project/data/pageviews_24h \
  /user/hadoop/single_project/input/pageviews_24h
```

## Run Hadoop MapReduce

```bash
/home/hadoop/single_project/scripts/run_hadoop_pageviews.sh \
  /home/hadoop/single_project/hadoop-pageviews \
  4 \
  /user/hadoop/single_project/input/pageviews_24h \
  /user/hadoop/single_project/intermediate/pageviews_etl \
  /user/hadoop/single_project/output/pageviews_analytics \
  20
```

Display output:

```bash
hdfs dfs -cat /user/hadoop/single_project/output/pageviews_analytics/part-r-*
```

## Run Sequential Baseline

```bash
/home/hadoop/single_project/scripts/run_sequential_pageviews.sh \
  /home/hadoop/single_project/data/pageviews_24h \
  /home/hadoop/single_project/results/sequential_pageviews.json \
  20
```

## Experimental Evaluation Plan

Recommended experiments:

1. Execution time vs dataset size:
   - 8 hourly files;
   - 16 hourly files;
   - 24 hourly files.

2. Hadoop configuration impact:
   - reducers = 1, 2, 4, 8.

3. Comparison against sequential Python baseline.

4. Optional resource metrics:
   - memory usage;
   - CPU utilization;
   - YARN elapsed time.

## Report Focus

The final report should focus on Hadoop MapReduce as the core implementation. The sequential Python version is used only as a non-parallel baseline for comparison.
