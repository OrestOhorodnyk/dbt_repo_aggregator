from __future__ import annotations

import pendulum
import logging
from airflow.models.dag import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from kubernetes.client import models as k8s


# --- DBT configuration ---
IMAGES = {
    "orders": "poc/dbt-orders:1.0.0", # dbt_repo_1
    "sales": "poc/dbt-sales:1.0.0", # dbt_repo_2
}

COMMANDS = {
    "seed": ["seed", "--profiles-dir", "/opt/dbt/profiles", "--project-dir", "/opt/dbt/project"],
    "run": ["run", "--profiles-dir", "/opt/dbt/profiles", "--project-dir", "/opt/dbt/project"],
}

ENV_VARS = {
    "DBT_HOST": "postgres-service",
    "DBT_USER": "dbt",
    "DBT_PASS": "dbt",
    "DBT_DBNAME": "analytics",
    "DBT_PORT": "5432",
}

# --- Example inline Python script ---
python_script = """
import time
import random
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.info("Hello from a Python script in a Kubernetes pod!")
logging.info(f"This is a multi-line script. The magic number is: {random.randint(1, 100)}")
time.sleep(5)
logging.info("Done.")
"""


# --- DAG Definition ---
with DAG(
    dag_id="run_multiple_db_repos",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["k8s", "dbt", "dynamic"],
) as dag:

    # --- Shared K8s volume mount ---
    dag_volume = k8s.V1Volume(
        name="dags-local-mount",
        host_path=k8s.V1HostPathVolumeSource(
            path="/Users/o.ohorodnyk/airflow-dags/dags",
            type="DirectoryOrCreate",
        ),
    )

    dag_volume_mount = k8s.V1VolumeMount(
        name="dags-local-mount",
        mount_path="/opt/airflow/dags",
        read_only=True,
    )

    # --- Dummy start & end tasks ---
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # --- Test connectivity task (Postgres) ---
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

    test_postgres = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
    )

    # --- Dynamically build dbt seed/run tasks ---
    dynamic_tasks = {}

    for project, image in IMAGES.items():
        previous_task = None

        for cmd_name, cmd_args in COMMANDS.items():
            task_id = f"{project}_{cmd_name}"

            task = KubernetesPodOperator(
                task_id=task_id,
                name=f"{project}-{cmd_name}-pod",
                namespace="airflow",
                image=image,
                cmds=["dbt"],
                arguments=cmd_args,
                env_vars=ENV_VARS,
                get_logs=True,
                is_delete_operator_pod=True,
                log_events_on_failure=True,
                do_xcom_push=False,
                volumes=[dag_volume],
                volume_mounts=[dag_volume_mount],
            )

            dynamic_tasks[task_id] = task

            # Chain seed → run for each project
            if previous_task:
                previous_task >> task
            previous_task = task

        # link each project's seed to previous tasks
        start >> test_postgres >> dynamic_tasks[f"{project}_seed"]
        # link last project task to end
        previous_task >> end
