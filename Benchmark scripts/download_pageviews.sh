#!/usr/bin/env bash
set -euo pipefail

BASE_URL="https://dumps.wikimedia.org/other/pageviews/2026/2026-05"
DAY="20260501"
HOURS="${1:-24}"
TARGET_DIR="${2:-/home/hadoop/single_project/data/pageviews_${HOURS}h}"

mkdir -p "$TARGET_DIR"

for i in $(seq 0 $((HOURS - 1))); do
  hour=$(printf "%02d" "$i")
  file="pageviews-${DAY}-${hour}0000.gz"
  url="${BASE_URL}/${file}"
  if [ ! -f "${TARGET_DIR}/${file}" ]; then
    echo "Downloading ${file}"
    wget -q --show-progress -O "${TARGET_DIR}/${file}" "$url"
  else
    echo "Already exists: ${file}"
  fi
done

du -sh "$TARGET_DIR"
