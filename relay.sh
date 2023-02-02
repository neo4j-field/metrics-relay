#!/bin/sh
set -e

# Figure out where our script is, allowing us to be run from anywhere.
WORKDIR="$(dirname $(realpath $0))"

# Check for virtualenv.
if [ ! -d "${WORKDIR}/venv" ]; then
	>&2 echo "failed to find virtual environment (venv)"
	exit 1
fi

# It's Morbin time...
exec "${WORKDIR}/venv/bin/python" "${WORKDIR}/main.py" $@ 
