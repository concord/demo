#!/bin/bash --login

set -e
cur_dir=$(pwd)
git_root=$(git rev-parse --show-toplevel)
build_dir=$git_root/liberty/build
nprocs=$(grep -c ^processor /proc/cpuinfo)

mkdir -p $build_dir

if [ ! -e $build_dir/word_counter.json ]; then
    cp $build_dir/../deploy_configs/*.json $build_dir
fi

cd $build_dir
if [ ! -e $build_dir/Makefile ]; then
    cmake $build_dir/..
fi
make -j$nprocs
cd $cur_dir
