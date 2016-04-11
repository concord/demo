#!/bin/bash --login
set -ex
git_root=$(git rev-parse --show-toplevel)

targets="$(gsutil ls gs://ephemeral-public/pre_processed_files/run*.tar.gz)"
#targets=(run-1-bucket-match.tar.gz)


join_csv=$git_root/liberty/build/join_csv
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
                    if [[ ! -e "${dir}/latency.csv" ]]; then
                        find "$dir" -iname "*latencies*.txt" \
                             -exec cat {} >> "$dir/latency.agg" \;
                        $latency_csv --latency_file "$dir/latency.agg" \
                                     --output_csv "$dir/latency.csv" \
                                     --logtostderr=1 \
                                     --v=1
                        echo "Produced latency file: ${dir}/latency.csv"
                    fi
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
            if [[ ! -e "$slave_dir/throughput.csv" ]]; then
                find "$slave_dir" -iname "*throughput.txt" -exec \
                     cat {} >> "$slave_dir/throughput.agg" \;
                $git_root/liberty/build/throughput_csv \
                    --throughput_file "$slave_dir/throughput.agg" \
                    --output_csv "$slave_dir/throughput.csv" \
                    --logtostderr=1 \
                    --v=1
                echo "Produced $slave_dir/throughput.csv"
            fi
        fi
    done
}

function process_hardware() {
    local work_dir=$1
    hw_files=($(find "$work_dir" -iname "hardware*.txt" -print))
    for file in "${hw_files[@]}"; do
        echo "Processing hardware: $file"
        output_dir=$(dirname "$file")
	$hardware_csv --hardware_file="$file" --output_csv="$output_dir/hardware.csv"
    done
}

function aggregate() {
    local work_dir=$1
    cd $work_dir
    local hw_files=$(find "$work_dir" -iname "hardware.csv")
    hw_files=$(echo "$hw_files" | tr '\n' ',')
    local latency_files=$(find "$work_dir" -iname "latency.csv" -print)
    latency_files=$(echo "$latency_files" | tr '\n' ',')
    local throughput_files=$(find "$work_dir" -iname "throughput.csv" -print)
    throughput_files=$(echo "$throughput_files" | tr '\n' ',')
    $join_csv --hardware_files="$hw_files" \
              --latency_files="$latency_files" \
              --throughput_files="$throughput_files" \
              --logtostderr=1 --v=1
}

if [[ ! -e $join_csv ]]; then
    $git_root/liberty/compile.bash
fi
for target in ${targets[@]}; do
    work_dir=$(mktemp -d)
    cd $work_dir
    echo "Downloading $target"
    gsutil -m cp $target "$work_dir"
    tar -xzf $work_dir/$(basename $target) -C "$work_dir"
    process_latencies $work_dir
    process_throughput $work_dir
    process_hardware $work_dir
    aggregate $work_dir
    gsutil -m cp -a public-read "$work_dir/aggregates.csv" "${target}_aggregates.csv"
done
