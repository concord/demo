#!/bin/bash --login
set -e
# tar_file="$1"
# work_dir=$(mktemp -d)
# cd $work_dir
# gsutil -m cp "gs://ephemeral-public/$tar_file" "$work_dir"
# tar -xzf "$work_dir/$tar_file" -C "$work_dir"


work_dir="/tmp/tmp.1v5m77lvWS"
cd $work_dir


for f in $(ls "$work_dir"); do
    f="$work_dir/$f"
    if [[ -d "$f" ]]; then
        for dir in $(ls "$f"); do
            dir="$f/$dir"
            find "$dir" -iname "latency.csv" -print
        done
    fi
done
