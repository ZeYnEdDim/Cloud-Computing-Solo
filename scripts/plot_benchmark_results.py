#!/usr/bin/env python3
import csv
import re
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
RESULT_DIR = ROOT / "results" / "benchmark"
PLOT_DIR = RESULT_DIR / "plots"
PLOT_DIR.mkdir(parents=True, exist_ok=True)


def read_csv(name):
    path = RESULT_DIR / name
    if not path.exists():
        print(f"Skipping {name}: file not found")
        return []
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def annotate_bars(ax, custom_labels=None):
    for index, container in enumerate(ax.containers):
        if custom_labels and index in custom_labels:
            labels = custom_labels[index]
        else:
            labels = []
            for bar in container:
                height = bar.get_height()
                labels.append(f"{height:.0f}" if height >= 10 else f"{height:.1f}")
        ax.bar_label(container, labels=labels, padding=3, fontsize=7)


def parse_log_elapsed_seconds(log_name):
    path = RESULT_DIR / "logs" / Path(log_name).name
    if not path.exists():
        return None

    pattern = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),")
    first = None
    last = None
    with path.open(encoding="utf-8", errors="ignore") as f:
        for line in f:
            match = pattern.match(line)
            if not match:
                continue
            stamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
            if first is None:
                first = stamp
            last = stamp

    if first is None or last is None:
        return None
    return int((last - first).total_seconds())


def add_failed_dataset_rows(rows):
    existing = {row["dataset"] for row in rows}
    failed_rows = read_csv("failed_runs_execution_by_dataset.csv")

    for failed in failed_rows:
        dataset = failed.get("dataset", "")
        if dataset in existing:
            continue
        if failed.get("benchmark") != "execution_by_dataset":
            continue
        if failed.get("reducers") != "sequential":
            continue

        hadoop_log = f"execution_dataset_{dataset}_hadoop.log"
        hadoop_time = parse_log_elapsed_seconds(hadoop_log)
        if hadoop_time is None:
            continue

        rows.append({
            "dataset": dataset,
            "hadoop_reducers": "4",
            "hadoop_time_seconds": str(hadoop_time),
            "sequential_time_seconds": failed.get("elapsed_seconds", "0"),
            "sequential_status": failed.get("status", "failed"),
        })

    return rows


def plot_execution_by_dataset():
    rows = read_csv("execution_time_by_dataset.csv")
    if not rows:
        return

    for row in rows:
        row.setdefault("sequential_status", "completed")
    rows = add_failed_dataset_rows(rows)

    order = {"1h": 1, "2h": 2, "4h": 4, "8h": 8, "16h": 16, "24h": 24}
    rows.sort(key=lambda row: order.get(row["dataset"], 999))

    datasets = [row["dataset"] for row in rows]
    hadoop = [float(row["hadoop_time_seconds"]) for row in rows]
    sequential = [float(row["sequential_time_seconds"]) for row in rows]
    seq_status = [row.get("sequential_status", "completed") for row in rows]

    x = range(len(datasets))
    width = 0.36

    fig, ax = plt.subplots(figsize=(10, 5.5))
    ax.bar([i - width / 2 for i in x], hadoop, width, label="Hadoop", color="#dd8452")
    seq_bars = ax.bar([i + width / 2 for i in x], sequential, width, label="Sequential", color="#55a868")

    for bar, status in zip(seq_bars, seq_status):
        if status != "completed":
            bar.set_hatch("//")
            bar.set_edgecolor("#222222")
            bar.set_linewidth(1.0)

    ax.set_title("Execution Time by Dataset Size")
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("Execution time (seconds)")
    ax.set_xticks(list(x), datasets)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    ax.set_ylim(0, max(max(hadoop), max(sequential)) * 1.18)
    hadoop_labels = [f"{value:.0f}" for value in hadoop]
    seq_labels = []
    for value, status in zip(sequential, seq_status):
        if status != "completed":
            seq_labels.append(f"{value:.0f}\\n(killed)")
        else:
            seq_labels.append(f"{value:.0f}")
    annotate_bars(ax, {0: hadoop_labels, 1: seq_labels})

    fig.tight_layout()
    fig.savefig(PLOT_DIR / "execution_time_by_dataset.png", dpi=200)
    plt.close(fig)


def plot_execution_by_reducers():
    rows = read_csv("execution_time_by_reducers.csv")
    rows = [row for row in rows if row.get("hadoop_time_seconds") not in ("", "N/A", None)]
    if not rows:
        return

    reducers = [row["reducers"] for row in rows]
    times = [float(row["hadoop_time_seconds"]) for row in rows]

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.bar(reducers, times, color="#4c72b0")
    ax.set_title("Hadoop Execution Time by Number of Reducers")
    ax.set_xlabel("Reducers")
    ax.set_ylabel("Execution time (seconds)")
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    annotate_bars(ax)
    fig.tight_layout()
    fig.savefig(PLOT_DIR / "execution_time_by_reducers.png", dpi=200)
    plt.close(fig)


def plot_memory_by_dataset():
    rows = read_csv("memory_by_dataset.csv")
    if not rows:
        return

    datasets = [row["dataset"] for row in rows]
    hadoop = [float(row["hadoop_avg_allocated_mb"]) for row in rows]
    sequential = [float(row["sequential_peak_rss_mb"]) for row in rows]

    x = range(len(datasets))
    width = 0.36

    fig, ax = plt.subplots(figsize=(9, 5.2))
    ax.bar([i - width / 2 for i in x], hadoop, width, label="Hadoop avg allocated MB", color="#dd8452")
    ax.bar([i + width / 2 for i in x], sequential, width, label="Sequential peak RSS MB", color="#55a868")
    ax.set_title("Memory Usage by Dataset Size")
    ax.set_xlabel("Dataset size")
    ax.set_ylabel("Memory (MB)")
    ax.set_xticks(list(x), datasets)
    ax.legend()
    ax.grid(axis="y", linestyle="--", alpha=0.35)
    annotate_bars(ax)
    fig.tight_layout()
    fig.savefig(PLOT_DIR / "memory_by_dataset.png", dpi=200)
    plt.close(fig)


def main():
    plot_execution_by_dataset()
    plot_execution_by_reducers()
    plot_memory_by_dataset()
    print(f"Plots saved in: {PLOT_DIR}")


if __name__ == "__main__":
    main()


