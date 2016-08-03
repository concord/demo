#!/bin/bash
set -x
venv=$(which virtualenv)
venv_name="$(basename $(git rev-parse --show-toplevel))_venv"
venv_path=$(git rev-parse --show-toplevel)/innovatively/${venv_name}
bash -c "virtualenv --relocatable $venv_path"
