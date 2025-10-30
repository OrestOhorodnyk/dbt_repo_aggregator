from __future__ import annotations

import pendulum
import logging
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


# --- DBT Project Configurations (submodules under /opt/airflow/dags/repo/dags) ---
DBT_PROJECTS = {
    "dbt_repo_1": "/opt/airflow/dags/repo/dags/dbt_repo_1/postgres_dbt_project",
    "dbt_repo_2": "/opt/airflow/dags/repo/dags/dbt_repo_2/sales_dbt_project",
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
)

# --- Test PostgreSQL connectivity before running dbt ---
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
        # executor_config=K8S_EXECUTOR_CONFIG,
    )

    # Dynamically create a DbtTaskGroup for each submodule
    dbt_groups = []

    for project_name, project_path in DBT_PROJECTS.items():
        dbt_group = DbtTaskGroup(
            group_id=project_name,
            project_config=ProjectConfig(
                project_name=project_name,
                dbt_project_path=project_path,  # Use dbt_project_path, not project_path
            ),
            profile_config=profile_config,
            execution_config=execution_config,
        )
        dbt_groups.append(dbt_group)
        start >> test_postgres >> dbt_group >> end