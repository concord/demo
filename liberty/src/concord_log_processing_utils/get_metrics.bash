#!/bin/bash --login

# Call python to download scripts
set -e
git_root=$(git rev-parse --show-toplevel)
utils_dir=$git_root/liberty/src/concord_log_processing_utils

work_dir=$(mktemp -d)
python $utils_dir/fetch_results.py -m $1 -r ".*.txt" -o $work_dir

function process_throughput() {
    for slave_dir in $(ls "$work_dir"); do
	if [[ -d "$slave_dir" ]]; then
	    find $dir -iname "*thoughput.txt" -exec \
		 cat {} >> "$slave_dir/throughput.agg" \;
       	    $git_root/liberty/build/throughput_csv \
		--throughput_file "$dir/throughput.agg" \
		--output_csv "$dir/throughput.csv" \
		--logtostderr=1 \
		--v=1
	    echo "Produced ${dir}/throughput.csv"
	fi
    done
}

function process_latencies() {
    for slave_dir in $(ls "$work_dir"); do
	echo "inside here ${slave_dir}"
	if [[ -d "$work_dir/$slave_dir" ]]; then
	    echo "inside here2"
	    for dir in $(ls "${slave_dir}"); do
		echo "here3"
		if [[ -d "${dir}" ]]; then
		    find "$dir" -iname "*.txt" -exec cat {} >> "$dir/latency.agg" \;
		    $git_root/liberty/build/latency_csv \
			--latency_file "$dir/latencies.agg" \
			--output_csv $"dir/latency.csv" \
			--logtostderr=1 \
			--v=1
		    echo "Produced latency file: ${dir}/latency.csv"
		fi
	    done
	fi
    done
}

process_latencies
process_throughput
