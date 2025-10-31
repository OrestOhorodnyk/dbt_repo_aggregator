from __future__ import annotations

import pendulum
from pathlib import Path
import logging
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    ExecutionConfig,
    RenderConfig,
    ExecutionMode,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping


# --- DBT Project Configurations ---
BASE_DIR = Path(__file__).resolve().parent

# Project paths on the local filesystem (for RenderConfig)
AIRFLOW_PROJECT_DIR = BASE_DIR

# Project paths inside Kubernetes pods (for ExecutionConfig)
K8S_PROJECT_DIR = "/opt/airflow/dags"

DBT_PROJECTS = {
    "dbt_repo_1": {
        "project_path": "dbt_repo_1/postgres_dbt_project",
        "image": "poc/dbt-orders:1.0.0",
    },
    "dbt_repo_2": {
        "project_path": "dbt_repo_2/sales_dbt_project",
        "image": "poc/dbt-sales:1.0.0",
    },
}

# --- DBT Profile Configuration (shared for all projects) ---
profile_config = ProfileConfig(
    profile_name="postgres_profile",
    target_name="prod",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",  # Airflow connection ID
        profile_args={"schema": "analytics"},
    ),
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
    dag_id="run_multiple_dbt_projects_k8s_cosmos",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "cosmos", "kubernetes", "dynamic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
    )

    # Dynamically create DbtTaskGroup for each project using Kubernetes execution mode
    dbt_groups = []

    for project_name, project_config in DBT_PROJECTS.items():
        project_path = project_config["project_path"]
        dbt_image = project_config["image"]
        
        # Local project path (for RenderConfig - where Airflow reads the project)
        local_project_path = str(AIRFLOW_PROJECT_DIR / project_path)
        
        # Kubernetes project path (for ExecutionConfig - where dbt runs inside pods)
        k8s_project_path = f"{K8S_PROJECT_DIR}/{project_path}"
        
        # RenderConfig: where the project is located for rendering (local filesystem)
        render_config = RenderConfig(
            dbt_project_path=local_project_path,
        )
        
        # ExecutionConfig: Kubernetes execution mode with project path in pods
        execution_config = ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
            dbt_project_path=k8s_project_path,
        )
        
        # Operator args: Kubernetes-specific options for KubernetesPodOperator
        operator_args = {
            "image": dbt_image,
            "get_logs": True,
            "is_delete_operator_pod": True,
            "namespace": "airflow",
            "log_events_on_failure": True,
        }
        
        # Create DbtTaskGroup with Kubernetes execution
        # Note: When using RenderConfig.dbt_project_path, ProjectConfig.dbt_project_path must be omitted
        # Project name can be derived from the project structure
        dbt_group = DbtTaskGroup(
            group_id=project_name,
            project_config=ProjectConfig(),  # Empty config - paths come from RenderConfig and ExecutionConfig
            profile_config=profile_config,
            render_config=render_config,
            execution_config=execution_config,
            operator_args=operator_args,
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
