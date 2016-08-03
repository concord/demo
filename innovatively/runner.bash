#!/bin/bash
set -x
echo "Directory: $(pwd)"
source demo_venv/bin/activate
exec python "$@"
