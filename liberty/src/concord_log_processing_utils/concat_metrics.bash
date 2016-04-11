#!/bin/bash --login
set -e
git_root=$(git rev-parse --show-toplevel)
tar_file="$1"
work_dir=$(mktemp -d)
cd $work_dir
gsutil -m cp "gs://ephemeral-public/$tar_file" "$work_dir"
tar -xzf "$work_dir/$tar_file" -C "$work_dir"
files=$(find "$work_dir" -iname "latency.csv" -print)
files=$(echo "$files" | tr '\n' ',')
$git_root/build/join_latency_csv --latency_files="$files" --output_csv="$work_dir/latency_aggregate.csv"
