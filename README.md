# Cloud Computing Solo Project - Wikimedia Pageview Analytics

Single-student Cloud Computing project based on Hadoop MapReduce and a Python non-parallel baseline. The project analyzes Wikimedia hourly pageview dumps and compares Hadoop execution with sequential local execution.

## Dataset

Dataset source:

https://dumps.wikimedia.org/other/pageviews/2026/2026-05/

Selected data:

- Day: 2026-05-01
- Format: hourly compressed Wikimedia pageview files
- Main benchmark subsets: 1h, 4h, and 8h
- Additional stress test: 16h

Dataset sizes used in the experiments:

| Subset | Hourly files | Compressed size | Decompressed text size | HDFS storage with replication |
|---|---:|---:|---:|---:|
| 1h | 1 | 51.7 MiB | 185.6 MiB | 103.5 MiB |
| 4h | 4 | 199.9 MiB | 723.2 MiB | 399.8 MiB |
| 8h | 8 | 400.6 MiB | 1.42 GiB | 801.3 MiB |
| 16h | 16 | 845.4 MiB | 2.97 GiB | 1.7 GiB |

The files are stored as gzip-compressed dumps. Hadoop stores the compressed files in HDFS, but the MapReduce jobs process the decompressed text records. HDFS storage is higher because the cluster uses replication factor 2.

Each input line has the following format:

```text
project_code page_title view_count response_size
```

Example:

```text
en Main_Page 42 50043
```

## Analytics Workflow

The Hadoop implementation is composed of two non-iterative MapReduce jobs in cascade.

### Job 1 - ETL and Classification

The first job parses raw Wikimedia pageview records and writes a structured intermediate dataset using a custom Hadoop `Writable`.

It performs:

- invalid line filtering;
- numeric parsing of the view count;
- extraction of the hour from the input filename;
- serialization into a SequenceFile intermediate dataset.

### Job 2 - Aggregation and Ranking

The second job performs distributed analytics using in-mapper combining and reducers. It aggregates page views, ranks the most visited projects and pages, and summarizes traffic by hour.

It computes:

- total views;
- top 10 projects by views;
- top 10 pages by views;
- views by hour;

Hadoop features used:

- two MapReduce jobs in cascade;
- Java implementation;
- custom `Writable`;
- in-mapper combining;
- custom partitioner;
- `setup()` and `cleanup()` methods;
- multiple reducers.

## Project Structure

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
  benchmark_execution_by_dataset.sh
  benchmark_execution_by_reducers.sh
  benchmark_memory_by_dataset.sh
  fetch_benchmark_results.ps1
  plot_benchmark_results.py
results/benchmark/
  execution_time_by_dataset.csv
  execution_time_by_reducers.csv
  memory_by_dataset.csv
  plots/
docs/
  documentation.tex
  documentation.pdf
presentation/
  Cloud_Computing_Solo_Project_Presentation.pptx
```

## VM and HDFS Paths

On the VM, the project is stored under:

```bash
/home/hadoop/single_project
```

Main HDFS base path:

```bash
/user/hadoop/single_project
```

Typical HDFS paths:

```bash
/user/hadoop/single_project/input/pageviews_1h
/user/hadoop/single_project/input/pageviews_4h
/user/hadoop/single_project/input/pageviews_8h
/user/hadoop/single_project/intermediate/...
/user/hadoop/single_project/output/...
```

## Setup on VM

Copy the project to the VM under `/home/hadoop/single_project`, then run:

```bash
chmod +x /home/hadoop/single_project/scripts/*.sh
cd /home/hadoop/single_project/hadoop-pageviews
mvn clean package
```

## Running Hadoop MapReduce

Example run with 4 reducers:

```bash
/home/hadoop/single_project/scripts/run_hadoop_pageviews.sh \
  /home/hadoop/single_project/hadoop-pageviews \
  4 \
  /user/hadoop/single_project/input/pageviews_8h \
  /user/hadoop/single_project/intermediate/pageviews_etl_8h \
  /user/hadoop/single_project/output/pageviews_analytics_8h \
  10
```

Display output:

```bash
hdfs dfs -cat /user/hadoop/single_project/output/pageviews_analytics_8h/part-r-*
```

## Running Sequential Baseline

```bash
/home/hadoop/single_project/scripts/run_sequential_pageviews.sh \
  /home/hadoop/single_project/data/pageviews_8h \
  /home/hadoop/single_project/results/benchmark/sequential_8h.json \
  10
```

## Benchmarks

The final experiments are separated into three scripts.

### 1. Execution Time by Dataset Size

Parameters:

- datasets: 1h, 4h, 8h;
- Hadoop reducers: 4;
- sequential baseline: same input data.

Run on VM:

```bash
cd /home/hadoop/single_project
./scripts/benchmark_execution_by_dataset.sh
```

Final results:

| Dataset | Hadoop, 4 reducers (s) | Sequential (s) |
|---|---:|---:|
| 1h | 117 | 20 |
| 4h | 295 | 75 |
| 8h | 583 | 154 |

A 16h stress test was also executed. Hadoop completed in about 1151 seconds, while the sequential process was killed after about 2245 seconds because of memory pressure.

### 2. Execution Time by Reducers

Parameters:

- dataset: 4h;
- reducers: 1, 2, 4, 8;
- Hadoop only.

Run on VM:

```bash
cd /home/hadoop/single_project
./scripts/benchmark_execution_by_reducers.sh
```

Final results:

| Reducers | Hadoop time (s) |
|---:|---:|
| 1 | 270 |
| 2 | 306 |
| 4 | 308 |
| 8 | 319 |

### 3. Memory Usage by Dataset Size

Parameters:

- datasets: 1h, 4h, 8h;
- Hadoop reducers: 4;
- sequential baseline: same input data.

Run on VM:

```bash
cd /home/hadoop/single_project
./scripts/benchmark_memory_by_dataset.sh
```

Final results:

| Dataset | Hadoop avg allocated MB | Sequential peak RSS MB |
|---|---:|---:|
| 1h | 2277.53 | 991.88 |
| 4h | 3051.64 | 2235.91 |
| 8h | 3334.44 | 3974.96 |

Note: Hadoop memory is measured as average YARN allocated memory, while sequential memory is measured as peak process RSS. They are useful for trend comparison, but they are not exactly the same metric.

## Fetching Results and Creating Plots

From the local machine:

```powershell
cd "C:\Users\BAKU\Desktop\University\Projects\Cloud Computing Solo"
powershell -ExecutionPolicy Bypass -File ".\scripts\fetch_benchmark_results.ps1"
python ".\scripts\plot_benchmark_results.py"
```

Generated plots:

```text
results/benchmark/plots/execution_time_by_dataset.png
results/benchmark/plots/execution_time_by_reducers.png
results/benchmark/plots/memory_by_dataset.png
```

## Documentation and Presentation

The final documentation is available at:

```text
docs/documentation.pdf
```

The LaTeX source is:

```text
docs/documentation.tex
```

The project presentation is available at:

```text
docs/Cloud_Computing_Solo_Project_Presentation.pptx
```

## Main Conclusion

On small and medium datasets, the Python sequential baseline is faster because the single-VM pseudo-distributed Hadoop setup introduces startup, shuffle, and HDFS I/O overhead. However, the 16h stress test shows the scalability limit of the sequential approach: Hadoop completes, while the Python process is killed under memory pressure.




