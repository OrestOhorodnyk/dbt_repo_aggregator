#!/bin/bash
set -e

echo "Running dbt inside container..."
cd "$DBT_PROJECT_DIR"

dbt debug --profiles-dir "$DBT_PROFILES_DIR"
dbt seed --profiles-dir "$DBT_PROFILES_DIR"
dbt run --profiles-dir "$DBT_PROFILES_DIR"
dbt test --profiles-dir "$DBT_PROFILES_DIR"
