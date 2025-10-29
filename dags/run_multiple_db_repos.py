from __future__ import annotations

import pendulum
import logging
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Kubernetes executor config to install packages
K8S_EXECUTOR_CONFIG = {
    "pod_override": {
        "spec": {
            "initContainers": [{
                "name": "install-packages",
                "image": "apache/airflow:3.0.6",
                "command": ["/bin/bash", "-c"],
                "args": [
                    "pip install --target /shared-packages --no-deps astronomer-cosmos==1.11.0 && "
                    "pip install --target /shared-packages aenum deprecation msgpack pydantic && "
                    "pip install --target /shared-packages dbt-core==1.8.8 dbt-postgres==1.8.2 && "
                    "echo 'Packages installed'"
                ],
                "volumeMounts": [{
                    "name": "shared-packages",
                    "mountPath": "/shared-packages"
                }]
            }],
            "containers": [{
                "name": "base",
                "env": [{
                    "name": "PYTHONPATH",
                    "value": "/shared-packages:/home/airflow/.local/lib/python3.12/site-packages"
                }],
                "volumeMounts": [{
                    "name": "shared-packages",
                    "mountPath": "/shared-packages"
                }]
            }],
            "volumes": [{
                "name": "shared-packages",
                "emptyDir": {}
            }]
        }
    }
}


# --- DBT Project Configurations (submodules under /opt/airflow/dags) ---
DBT_PROJECTS = {
    "dbt_repo_1": "/opt/airflow/dags/dbt_repo_1",
    "dbt_repo_2": "/opt/airflow/dags/dbt_repo_2",
}

# --- DBT Profile Configuration (shared for all projects) ---
profile_config = ProfileConfig(
    profile_name="default",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",  # Airflow connection ID
        profile_args={"schema": "analytics"},  # default schema
    ),
)

# --- Optional: dbt execution settings ---
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",  # path inside your image
    # Note: retries should be set at the task level, not in ExecutionConfig
)

# --- Test PostgreSQL connectivity before running dbt ---
def test_postgres_conn(**kwargs):
    logging.info("Testing PostgreSQL connection via PostgresHook...")
    hook = PostgresHook(postgres_conn_id="postgres_dbt")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    logging.info(f"✅ Connected successfully! PostgreSQL version: {version}")
    cursor.close()
    conn.close()


# --- DAG Definition ---
with DAG(
    dag_id="run_multiple_dbt_projects_cosmos",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "cosmos", "dynamic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
        executor_config=K8S_EXECUTOR_CONFIG,
    )

    # Dynamically create a DbtTaskGroup for each submodule
    dbt_groups = []

    for project_name, project_path in DBT_PROJECTS.items():
        dbt_group = DbtTaskGroup(
            group_id=project_name,
            project_config=ProjectConfig(
                project_name=project_name,
                project_path=project_path,
            ),
            profile_config=profile_config,
            execution_config=execution_config,
        )
        dbt_groups.append(dbt_group)
        start >> test_postgres >> dbt_group >> end
