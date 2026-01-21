#!/bin/bash

set -e

cd /Users/krish/Odyn/rental_platform/postgres_vast
source /Users/krish/Odyn/rental_platform/.venv/bin/activate
python ingestion.py >> cron.log 2>&1
python ingest_runpod.py >> cron.log 2>&1
