from __future__ import annotations

import pendulum
from pathlib import Path
import logging
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s


# --- DBT Project Configurations ---
BASE_DIR = Path(__file__).resolve().parent
DBT_PROJECTS = {
    "dbt_repo_1": {
        "image": "poc/dbt-orders:1.0.0",
        "project_path": "dbt_repo_1/postgres_dbt_project",
    },
    "dbt_repo_2": {
        "image": "poc/dbt-sales:1.0.0",
        "project_path": "dbt_repo_2/sales_dbt_project",
    },
}

# dbt commands to run for each project
DBT_COMMANDS = {
    "seed": ["seed", "--profiles-dir", "/opt/dbt/profiles", "--project-dir", "/opt/dbt/project"],
    "run": ["run", "--profiles-dir", "/opt/dbt/profiles", "--project-dir", "/opt/dbt/project"],
}

# Environment variables for dbt (passed to containers)
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
    dag_id="run_multiple_dbt_projects_k8s_pod_operator",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "docker", "kubernetes", "dynamic"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
    )

    # Shared volume mount for DAGs (if needed)
    dag_volume = k8s.V1Volume(
        name="dags-volume",
        host_path=k8s.V1HostPathVolumeSource(
            path=str(BASE_DIR.parent),  # Parent directory containing dags/
            type="DirectoryOrCreate",
        ),
    )

    dag_volume_mount = k8s.V1VolumeMount(
        name="dags-volume",
        mount_path="/opt/airflow/dags",
        read_only=True,
    )

    # Dynamically create KubernetesPodOperator tasks for each dbt project
    project_tasks = {}

    for project_name, project_config in DBT_PROJECTS.items():
        docker_image = project_config["image"]
        project_path = project_config["project_path"]
        
        project_task_list = []
        previous_task = None
        
        # Create seed and run tasks for each project
        for cmd_name, cmd_args in DBT_COMMANDS.items():
            task_id = f"{project_name}_{cmd_name}"
            
            task = KubernetesPodOperator(
                task_id=task_id,
                name=f"{project_name}-{cmd_name}-pod",
                namespace="airflow",
                image=docker_image,
                cmds=["dbt"],
                arguments=cmd_args,
                env_vars=DBT_ENV_VARS,
                get_logs=True,
                is_delete_operator_pod=True,
                log_events_on_failure=True,
                do_xcom_push=False,
                volumes=[dag_volume],
                volume_mounts=[dag_volume_mount],
            )
            
            project_task_list.append(task)
            
            # Chain seed -> run for each project
            if previous_task:
                previous_task >> task
            previous_task = task
        
        project_tasks[project_name] = project_task_list

    # Chain all projects sequentially: start -> test_postgres -> dbt_repo_1 -> dbt_repo_2 -> end
    previous_project_tasks = None
    
    for project_name, tasks in project_tasks.items():
        first_task = tasks[0]
        last_task = tasks[-1]
        
        if previous_project_tasks:
            # Chain last task of previous project to first task of current project
            previous_project_tasks[-1] >> first_task
        else:
            # First project: chain from test_postgres
            test_postgres >> first_task
        
        previous_project_tasks = tasks
    
    # Connect the last task to end
    if previous_project_tasks:
        previous_project_tasks[-1] >> end
    
    # Connect start to test_postgres
    start >> test_postgres
