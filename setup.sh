#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Creates the Python virtual environment (.venv) used for the CLIENT, the DEMO
# and the TESTS. Everything installs INSIDE .venv, so deleting that folder
# removes it all. (The controller/workers run in Docker and need no host install.)
#
#   ./setup.sh
#   source .venv/bin/activate
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

# The code uses modern type-hint syntax (e.g. `dict | None`) -> Python 3.10+.
if ! python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)'; then
  echo "Python 3.10 or newer is required (found: $(python3 --version 2>&1))." >&2
  exit 1
fi

python3 -m venv .venv

# PIP_NO_CACHE_DIR: by default pip keeps downloaded packages in ~/.cache/pip,
# OUTSIDE the venv, which would survive deleting .venv. Disabling the cache keeps
# the venv fully self-contained, so removing it really frees everything.
export PIP_NO_CACHE_DIR=1
.venv/bin/python -m pip install --quiet --upgrade pip
.venv/bin/python -m pip install --quiet -r requirements-dev.txt

echo "Done. Activate it with:   source .venv/bin/activate"
echo "Then try:                 python client/objstore_cli.py --help"
