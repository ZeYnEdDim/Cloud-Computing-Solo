#!/usr/bin/env bash
set -euo pipefail

INPUT="${1:-/home/hadoop/single_project/data/pageviews_24h}"
OUTPUT="${2:-/home/hadoop/single_project/results/sequential_pageviews.json}"
TOP_N="${3:-20}"

mkdir -p "$(dirname "$OUTPUT")"
python3 /home/hadoop/single_project/sequential/pageview_sequential.py \
  --input "$INPUT" \
  --output "$OUTPUT" \
  --top-n "$TOP_N"
