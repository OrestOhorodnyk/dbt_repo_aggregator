FROM python:3.11-slim

# Install system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential libpq-dev && \
    pip install --no-cache-dir --upgrade pip setuptools wheel

# Copy requirements and install
WORKDIR /opt/dbt
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /opt/dbt/project
COPY profiles /opt/dbt/profiles

# Environment variables for dbt
ENV DBT_PROJECT_DIR=/opt/dbt/project
ENV DBT_PROFILES_DIR=/opt/dbt/profiles
ENV DBT_HOST=postgres-service \
    DBT_USER=dbt \
    DBT_PASS=dbt \
    DBT_PORT=5432 \
    DBT_DBNAME=analytics \
    DBT_SCHEMA=public

# Optional: add entrypoint to run dbt automatically
COPY entrypoint.sh /usr/local/bin/dbt-entrypoint
RUN chmod +x /usr/local/bin/dbt-entrypoint
ENTRYPOINT ["/usr/local/bin/dbt-entrypoint"]
