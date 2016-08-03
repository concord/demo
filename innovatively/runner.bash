#!/bin/bash
set -x
echo "Directory: $(pwd)"
exec python "$@"
