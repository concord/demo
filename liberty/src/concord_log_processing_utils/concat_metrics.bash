#!/bin/bash --login
set -ex
git_root=$(git rev-parse --show-toplevel)
#targets=($(gsutil ls gs://ephemeral-public/run*.tar.gz))
targets=(run-1-bucket-match.tar.gz)
join_latencies=$git_root/liberty/build/join_latencies_csv
join_hardware=$git_root/liberty/build/join_hardware_csv
latency_csv=$git_root/liberty/build/latency_csv
hardware_csv=$git_root/liberty/build/hardware_csv

function process_latencies() {
    local work_dir=$1
    for slave_dir in $(ls "$work_dir"); do
	slave_dir="$work_dir/$slave_dir"
	if [[ -d "$slave_dir" ]]; then
	    for dir in $(ls "${slave_dir}"); do
		dir="$slave_dir/$dir"
		if [[ -d "${dir}" ]]; then
		    find "$dir" -iname "*.txt" -exec cat {} >> "$dir/latency.agg" \;
		    $latency_csv --latency_file "$dir/latency.agg" \
				 --output_csv "$dir/latency.csv" \
				 --logtostderr=1 \
				 --v=1
		    rm "$dir/latency.agg"
		    echo "Produced latency file: ${dir}/latency.csv"
		fi
	    done
	fi
    done
}

function process_throughput() {
    local work_dir=$1
    for slave_dir in $(ls "$work_dir"); do
	slave_dir="$work_dir/$slave_dir"
	if [[ -d "$slave_dir" ]]; then
	    find "$slave_dir" -iname "*throughput.txt" -exec \
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

function process_hardware() {
    local work_dir=$1
    files=($(find "$work_dir" -iname "hardware_usage_monitor.txt" -print))
    for file in "${files[@]}"
    do
	output_dir=$(dirname "$file")
	$hardware_csv --hardware_file="$file" --output_csv="$output_dir/hardware.csv"
    done
}

function aggreagte_hardware() {
    local work_dir=$1
    if [[ -z $(ls "$work_dir/hardware_aggregate.csv") ]]
    then
	return
    fi
    files=$(find "$work_dir" -iname "hardware.csv" -print)
    files=$(echo "$files" | tr '\n' ',')
    if [[ -z "$files" ]]
    then
	$join_hardware --hardware_file="$files" \
		       --output_csv="$work_dir/hardware_aggregate.csv"
    else
	process_hardware $work_dir
    fi    
}

function aggregate_throughput() {
    local work_dir=$1
    files=$(find "$work_dir" -iname "throughput.csv" -print)
    files=$(echo "$files" | tr '\n' ',')   
}

function aggregate_latency() {
    local work_dir=$1
    if [[ -z $(ls "$work_dir/latency_aggregate.csv") ]]
    then
	return
    fi    
    files=$(find "$work_dir" -iname "latency.csv" -print)
    files=$(echo "$files" | tr '\n' ',')
    if [[ -z "$files" ]]; then
	$join_latencies --latency_file="$files" \
			--output_csv="$work_dir/latency_aggregate.csv"
    else
	process_latencies $work_dir
    fi      
}

$git_root/liberty/compile.bash
for target in ${targets[@]}; do
    work_dir=$(mktemp -d)
    echo "Downloading $target"
    gsutil -m cp -r "gs://ephemeral-public/$target" "$work_dir"
    tar -xzf "$work_dir/$target" -C "$work_dir"
    #aggregate_latency $work_dir
    #aggregate_throughput $work_dir
    process_hardware $work_dir
done
