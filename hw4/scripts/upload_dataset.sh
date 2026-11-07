#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
CSV=${1:?путь к it_vacancies_2024.csv}
docker compose up -d rustfs
docker compose run --rm --no-deps -v "$CSV:/dataset/it_vacancies_2024.csv:ro" coord-a \
  python src/upload_dataset.py /dataset/it_vacancies_2024.csv
