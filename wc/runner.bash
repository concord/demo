#!/bin/bash
set -ex

SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
    exec_dir="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
    SOURCE="$(readlink "$SOURCE")"
    [[ $SOURCE != /* ]] && SOURCE="$exec_dir/$SOURCE"
    # if $SOURCE was a relative symlink, we need to resolve it
    # relative to the path where the symlink file was located
done
exec_dir="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
original=${PWD}
requirements=$(find $exec_dir -name requirements.txt)

echo "Mesos directory: ${PWD}"
echo "Exec directory: ${exec_dir}"

if [[ -f $requirements ]]; then
    # need to overcome pip 128 chars path - software... :'(
    work_dir=$(mktemp -d -p $original)
    symlink_dir=$(mktemp -d)
    ln -s $work_dir $symlink_dir/concord
    dir=$symlink_dir/concord
    cd $dir
    echo "Installing venv in $dir"
    virtualenv $dir/env
    $dir/env/bin/pip install -r $requirements
    cd $exec_dir
    exec $dir/env/bin/python "$original/$@"
else
    exec python "$original/$@"
fi

rc = $?
if [[ $rc != 0 ]]; then
    echo "Client exited with: $rc"
    exit $rc
fi
