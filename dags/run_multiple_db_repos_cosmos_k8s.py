from __future__ import annotations

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
        "image": "poc/dbt-orders:1.0.3",
        # Manifest path - sync_manifests_from_gcs writes to shared local_logs volume
        # This is accessible by all pods (scheduler, dagProcessor, etc.)
        "manifest": "/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json",
        "render_path": "/opt/airflow/dags/manifests/dbt_orders_stub",  # For RenderConfig
    },
    "dbt_repo_2": {
        "image": "poc/dbt-sales:1.0.1",
        # Manifest path - sync_manifests_from_gcs writes to shared local_logs volume
        # This is accessible by all pods (scheduler, dagProcessor, etc.)
        "manifest": "/opt/airflow/local_logs/manifests/dbt_sales_stub/target/manifest.json",
        "render_path": "/opt/airflow/dags/manifests/dbt_sales_stub",  # For RenderConfig
    },
}

profile_config = ProfileConfig(
    profile_name="postgres_profile",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_dbt",
        profile_args={"schema": "analytics"},
    ),
)




with DAG(
    dag_id="run_dbt_projects_from_docker_images",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dbt", "cosmos", "kubernetes"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    previous = start

    for repo, cfg in DBT_IMAGES.items():
        # Manifest path - must be local filesystem path
        # If using GCS, run sync_manifests_from_gcs.py before DAG deployment
        # Cosmos requires manifest at DAG parse time, not runtime
        manifest_path = cfg["manifest"]

        project_config = ProjectConfig(
            project_name=repo,
            manifest_path=manifest_path,
        )

        render_config = RenderConfig(
            dbt_project_path=cfg.get("render_path", f"/opt/airflow/dags/manifests/{repo}"),
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
                        # No volume mounts - entrypoint will write profiles.yml to /tmp/airflow_cosmos
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
                    # Set profiles directory to where ConfigMap is mounted
                    "DBT_PROFILES_DIR": "/tmp/airflow_cosmos",
                },
            },
        )

        previous >> run_models
        previous = run_models

    previous >> end
