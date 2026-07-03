#!/usr/bin/env python3
"""Sequential baseline for Wikimedia pageview analytics."""

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path


def open_text(path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return path.open("r", encoding="utf-8", errors="ignore")


def extract_hour(path):
    name = path.name
    if name.startswith("pageviews-") and len(name) >= 21:
        return f"{name[10:18]}-{name[19:21]}"
    return "unknown"


def iter_input_files(input_path):
    root = Path(input_path)
    if root.is_file():
        yield root
        return
    for path in sorted(root.glob("pageviews-*")):
        if path.is_file():
            yield path


def run(input_path, output_path, top_n):
    total_views = 0
    project_views = Counter()
    page_views = Counter()
    hour_views = Counter()

    for path in iter_input_files(input_path):
        hour = extract_hour(path)
        with open_text(path) as f:
            for line in f:
                parts = line.strip().split(" ", 4)
                if len(parts) < 3:
                    continue

                project = parts[0]
                page = parts[1]
                try:
                    views = int(parts[2])
                except ValueError:
                    continue

                if not project or not page or views <= 0:
                    continue

                total_views += views
                project_views[project] += views
                page_views[f"{project}|{page}"] += views
                hour_views[hour] += views

    result = {
        "summary": {
            "total_views": total_views,
        },
        "top_projects_by_views": [
            {"project": project, "views": views}
            for project, views in project_views.most_common(top_n)
        ],
        "top_pages_by_views": [
            {"page": page, "views": views}
            for page, views in page_views.most_common(top_n)
        ],
        "views_by_hour": [
            {"hour": hour, "views": hour_views[hour]}
            for hour in sorted(hour_views)
        ],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sequential Wikimedia pageview analytics")
    parser.add_argument("--input", required=True, help="Input file or directory containing pageviews files")
    parser.add_argument("--output", required=True, help="Output JSON path")
    parser.add_argument("--top-n", type=int, default=10)
    args = parser.parse_args()
    run(args.input, args.output, args.top_n)

