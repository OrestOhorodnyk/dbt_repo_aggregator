from __future__ import annotations

import pendulum
from pathlib import Path
import logging
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping


# --- DBT Project Configurations ---
BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECTS = {
    "dbt_repo_1": {
        "project_path": str(BASE_DIR / "dbt_repo_1/postgres_dbt_project"),
        "image": "poc/dbt-orders:1.0.0",
    },
    "dbt_repo_2": {
        "project_path": str(BASE_DIR / "dbt_repo_2/sales_dbt_project"),
        "image": "poc/dbt-sales:1.0.0",
    },
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

# Environment variables for dbt (will be passed to Docker containers)
DBT_ENV_VARS = {
    "DBT_HOST": "postgres-service",
    "DBT_USER": "dbt",
    "DBT_PASS": "dbt",
    "DBT_DBNAME": "analytics",
    "DBT_PORT": "5432",
}

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
    dag_id="run_multiple_dbt_projects_cosmos_docker",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "cosmos", "docker", "dynamic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
    )

    # Dynamically create a DbtTaskGroup for each project using Docker execution mode
    dbt_groups = []

    for project_name, project_config in DBT_PROJECTS.items():
        project_path = project_config["project_path"]
        docker_image = project_config["image"]
        
        # Project configuration
        project_cfg = ProjectConfig(
            project_name=project_name,
            dbt_project_path=project_path,
        )
        
        # Execution configuration for Docker
        execution_cfg = ExecutionConfig(
            execution_mode="docker",
            docker_image=docker_image,
            docker_env=DBT_ENV_VARS,
            # Mount the dbt project directory into the Docker container
            docker_mounts=[
                {
                    "source": project_path,
                    "target": "/opt/dbt/project",
                    "type": "bind"
                }
            ]
        )
        
        # Create DbtTaskGroup with Docker execution
        dbt_group = DbtTaskGroup(
            group_id=project_name,
            project_config=project_cfg,
            profile_config=profile_config,
            execution_config=execution_cfg,
        )
        dbt_groups.append(dbt_group)

    # Chain all tasks sequentially: start -> test_postgres -> dbt_group_1 -> dbt_group_2 -> ... -> end
    previous_task = test_postgres
    
    for dbt_group in dbt_groups:
        previous_task >> dbt_group
        previous_task = dbt_group
    
    # Connect to start and end
    start >> test_postgres
    previous_task >> end
