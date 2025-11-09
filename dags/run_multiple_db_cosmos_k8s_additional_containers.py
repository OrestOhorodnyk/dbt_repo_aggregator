import pendulum
from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from cosmos import (
    DbtTaskGroup,
    ProjectConfig,
    ProfileConfig,
    ExecutionConfig,
    RenderConfig,
    ExecutionMode,
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from kubernetes.client import models as k8s


DBT_IMAGES = {
    "dbt_repo_1": {
        "image": "poc/dbt-orders:1.0.5",
        "manifest": "/opt/airflow/dags/manifests/dbt_repo_1_manifest.json",
    },
    "dbt_repo_2": {
        "image": "poc/dbt-sales:1.0.5",
        "manifest": "/opt/airflow/dags/manifests/dbt_repo_2_manifest.json",
    },
}

profile_config = ProfileConfig(
    profile_name="postgres_profile", # profile name from dbt project/profiles/profiles.yml (first line)
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",      # airflow cinnection to connect to the target db
        profile_args={"schema": "analytics"},
    ),
)




with DAG(
    dag_id="run-dbt-projects-cosmos-k8s-additional-containers",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "cosmos", "kubernetes"],
    max_active_tasks=2,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    previous = start

    for repo, cfg in DBT_IMAGES.items():
        manifest_path = cfg["manifest"]

        project_config = ProjectConfig(
            project_name=repo,
            manifest_path=manifest_path,
        )

        render_config = RenderConfig(
            dbt_project_path=cfg.get("render_path", f"/opt/airflow/dags/manifests/{repo}__manifest.json"),
        )

        execution_config = ExecutionConfig(
            execution_mode=ExecutionMode.KUBERNETES,
            dbt_project_path="/opt/dbt",
        )

        pod_override = k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image=cfg["image"],
                    )
                ],
            ),
        )

        run_models = DbtTaskGroup(
            group_id=f"{repo}_run",
            project_config=project_config,
            profile_config=profile_config,
            render_config=render_config,
            execution_config=execution_config,
            operator_args={
                "image": cfg["image"],
                "namespace": "airflow",
                "get_logs": True,
                "is_delete_operator_pod": False,
                "log_events_on_failure": True,
                "in_cluster": True,
                # ✅ pod_override must be passed directly, not via executor_config
                "pod_override": pod_override,
                "env_vars": {
                    # Connection details from Airflow connection
                    "DBT_HOST": "postgres-service",
                    "DBT_USER": "dbt",
                    "DBT_PASS": "dbt",
                    "DBT_DBNAME": "analytics",
                    "DBT_PORT": "5432",
                },
            },
        )

        previous >> run_models
        previous = run_models

    previous >> end