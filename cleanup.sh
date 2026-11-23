#!/usr/bin/env bash
# -----------------------------------------------------------------------------
# Removes EVERYTHING this project put on your machine, so nothing lingers when
# the project is over:
#   * Docker: containers, network, the 5 named volumes (ALL STORED DATA),
#             and the two images we built
#   * the Python virtual environment (.venv), test caches and __pycache__ folders
#
#   ./cleanup.sh              ask for confirmation, remove all of the above
#   ./cleanup.sh -y           no confirmation
#   ./cleanup.sh --docker-only | --venv-only
#   ./cleanup.sh --base-image also remove the shared python:3.12-slim base image
#
# (Deleting this project folder afterwards removes the source code itself.)
# -----------------------------------------------------------------------------
set -euo pipefail
cd "$(dirname "$0")"

DO_DOCKER=1; DO_VENV=1; BASE=0; YES=0
for arg in "$@"; do
  case "$arg" in
    -y|--yes) YES=1 ;;
    --docker-only) DO_VENV=0 ;;
    --venv-only) DO_DOCKER=0 ;;
    --base-image) BASE=1 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 1 ;;
  esac
done

if [ "$YES" -eq 0 ]; then
  echo "This will permanently delete all stored objects (Docker volumes) and the .venv."
  read -r -p "Continue? [y/N] " answer
  [[ "$answer" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 1; }
fi

if [ "$DO_DOCKER" -eq 1 ]; then
  if command -v docker >/dev/null 2>&1; then
    # -v: delete named volumes   --rmi all: delete the images compose built
    docker compose down -v --rmi all --remove-orphans || true
    if [ "$BASE" -eq 1 ]; then docker rmi python:3.12-slim 2>/dev/null || true; fi
  else
    echo "docker not found - skipping Docker cleanup"
  fi
fi

if [ "$DO_VENV" -eq 1 ]; then
  rm -rf .venv .pytest_cache
  find . -type d -name __pycache__ -prune -exec rm -rf {} +
fi
echo "Cleanup complete."
