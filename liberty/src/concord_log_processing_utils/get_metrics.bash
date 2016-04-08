#!/bin/bash --login

if [[ $1 == "" || $2 == "" || $3 == "" ]]; then
    echo "Usage: ./get_metrics [master-ip] [run_no] [test_name]"
    exit 1
fi

set -e
git_root=$(git rev-parse --show-toplevel)
utils_dir=$git_root/liberty/src/concord_log_processing_utils

work_dir=$(mktemp -d)
python $utils_dir/fetch_results.py -m $1 -r ".*.txt" -o $work_dir

function process_throughput() {
    for slave_dir in $(ls "$work_dir"); do
	slave_dir="$work_dir/$slave_dir"
	if [[ -d "$slave_dir" ]]; then
	    find "$slave_dir" -iname "*thoughput.txt" -exec \
		 cat {} >> "$slave_dir/throughput.agg" \;
	    $git_root/liberty/build/throughput_csv \
		--throughput_file "$slave_dir/throughput.agg" \
		--output_csv "$slave_dir/throughput.csv" \
		--logtostderr=1 \
		--v=1
	    echo "Produced $slave_dir/throughput.csv"
	fi
    done
}

function process_latencies() {
    for slave_dir in $(ls "$work_dir"); do
	slave_dir="$work_dir/$slave_dir"
	if [[ -d "$slave_dir" ]]; then
	    for dir in $(ls "${slave_dir}"); do
		dir="$slave_dir/$dir"
		if [[ -d "${dir}" ]]; then
		    find "$dir" -iname "*.txt" -exec cat {} >> "$dir/latency.agg" \;
		    $git_root/liberty/build/latency_csv \
			--latency_file "$dir/latency.agg" \
			--output_csv "$dir/latency.csv" \
			--logtostderr=1 \
			--v=1
		    echo "Produced latency file: ${dir}/latency.csv"
		fi
	    done
	fi
    done
}

function cleanup() {
    local compressed_file="$work_dir/run-$1-$2.tar.gz"
    tar -czf "$compressed_file" \
	$(find "$work_dir" -iname "*.csv" -or -iname "hardware_usage_monitor.txt")
    gsutil -m cp -a public-read "$compressed_file" gs://ephemeral-public
    rm -rf "$work_dir"
}

process_latencies
process_throughput
cleanup $2 $3

