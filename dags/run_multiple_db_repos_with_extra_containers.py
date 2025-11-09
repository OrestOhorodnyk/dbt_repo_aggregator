import os
import pendulum
from pathlib import Path
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
import logging

# --- Resolve base path dynamically ---
DAGS_BASE = Path(__file__).resolve().parent.parent  # /opt/airflow/dags/repo/dags → /opt/airflow/dags/repo
DBT_BASE = Path("/opt/airflow/dags")  # where dbt-git-sync clones

DBT_PROJECTS = {
    "dbt_repo_1": str(DBT_BASE / "dbt_repo_1/postgres_dbt_project"),
    "dbt_repo_2": str(DBT_BASE / "dbt_repo_2/sales_dbt_project"),
}



# --- DBT Profile Configuration ---
profile_config = ProfileConfig(
    profile_name="default",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",
        profile_args={"schema": "public"},
    ),
)

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)

def test_postgres_conn(**kwargs):
    logging.info("Testing PostgreSQL connection via PostgresHook...")
    hook = PostgresHook(postgres_conn_id="postgres_dbt")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()
            logging.info(f"✅ Connected successfully! PostgreSQL version: {version}")

with DAG(
    dag_id="multiple_db_repos_with_extra_containers",
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dbt", "cosmos"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    test_pg = PythonOperator(
        task_id="test_postgres_connection",
        python_callable=test_postgres_conn,
    )

    dbt_groups = []
    for repo, path in DBT_PROJECTS.items():
        path_obj = Path(path)
        
        # Check if path exists and log details
        if not path_obj.exists():
            logging.error(f"❌ Skipping {repo}: Path does not exist: {path}")
            logging.error(f"   Current working directory: {Path.cwd()}")
            logging.error(f"   DAG file location: {Path(__file__).resolve()}")
            logging.error(f"   DBT_BASE exists: {DBT_BASE.exists()}")
            if DBT_BASE.exists():
                logging.error(f"   Contents of {DBT_BASE}: {list(DBT_BASE.iterdir())}")
            continue
        
        # Check if dbt_project.yml exists
        dbt_project_yml = path_obj / "dbt_project.yml"
        if not dbt_project_yml.exists():
            logging.warning(f"⚠️  {repo}: dbt_project.yml not found at {dbt_project_yml}")
            logging.warning(f"   Path exists but may not be a valid dbt project")
            # Continue anyway - let Cosmos handle the error
        
        logging.info(f"✅ Creating DbtTaskGroup for {repo} at {path}")
        
        try:
            dbt_group = DbtTaskGroup(
                group_id=repo,
                project_config=ProjectConfig(
                    project_name=repo,
                    dbt_project_path=path,
                ),
                profile_config=profile_config,
                execution_config=execution_config,
            )
            dbt_groups.append(dbt_group)
            # Count tasks in the task group
            task_count = len([t for t in dag.tasks if t.task_id.startswith(f"{repo}.")])
            logging.info(f"✅ Successfully created DbtTaskGroup for {repo} (tasks will be added to DAG)")
        except Exception as e:
            logging.error(f"❌ Failed to create DbtTaskGroup for {repo}: {e}")
            import traceback
            logging.error(traceback.format_exc())

    # chain tasks
    prev = start >> test_pg
    for g in dbt_groups:
        prev >> g
        prev = g
    prev >> end