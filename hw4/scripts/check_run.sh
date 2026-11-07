#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
docker compose run --rm --no-deps coord-a python src/check_run.py "$@"
