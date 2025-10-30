FROM apache/airflow:3.0.6

# Install Python deps as the airflow user per official docs
USER airflow
RUN pip install --no-cache-dir \
    astronomer-cosmos==1.11.0 \
    dbt-core==1.8.8 \
    dbt-postgres==1.8.2 \
    aenum deprecation msgpack pydantic

# Reassert Airflow core version to avoid accidental downgrade via deps
RUN pip install --no-cache-dir --upgrade "apache-airflow==3.0.6"


