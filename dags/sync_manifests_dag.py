from __future__ import annotations

import sys
from pathlib import Path
import pendulum
from airflow.models import DAG
from airflow.operators.python import PythonOperator

# Add utils directory to path to import sync script
UTILS_DIR = Path(__file__).parent / "utils"
sys.path.insert(0, str(UTILS_DIR))


def run_sync_manifests(**context):
    """
    Import and run the sync_manifests function from utils/sync_manifests_from_gcs.py.
    
    Raises an exception if sync fails, causing the task to fail.
    """
    try:
        from sync_manifests_from_gcs import sync_manifests, setup_gcp_credentials
        
        # Set up GCP credentials
        setup_gcp_credentials()
        
        # Run sync with raise_on_failure=True to fail task on any error
        synced_count, failed_count, failures = sync_manifests(raise_on_failure=True)
        
        # If we get here, sync was successful
        print(f"✅ Successfully synced {synced_count} manifest(s)")
        
        return {
            "synced_count": synced_count,
            "failed_count": failed_count,
            "status": "success"
        }
    except ImportError as e:
        error_msg = f"Could not import sync script: {e}"
        print(f"❌ {error_msg}")
        print("   Make sure google-cloud-storage or apache-airflow-providers-google is installed")
        raise
    except RuntimeError as e:
        # This is raised by sync_manifests() when sync fails
        error_msg = f"Sync failed: {e}"
        print(f"❌ {error_msg}")
        raise
    except Exception as e:
        error_msg = f"Unexpected error during sync: {e}"
        print(f"❌ {error_msg}")
        raise


with DAG(
    dag_id="sync_manifests_from_gcs",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="*/10 * * * *",  # Every 10 minutes
    catchup=False,
    tags=["sync", "gcs", "manifests"],
    description="Syncs dbt manifests from GCS bucket to local filesystem every 10 minutes",
    max_active_runs=1,  # Only one sync running at a time
) as dag:

    sync_task = PythonOperator(
        task_id="sync_manifests_from_gcs",
        python_callable=run_sync_manifests,
        doc_md="""
        Syncs dbt manifests from GCS bucket to local filesystem.
        
        This task:
        - Downloads manifests from GCS using gcp-key.json for authentication
        - Saves them to local paths used by the dbt DAGs
        - Runs every 10 minutes to keep manifests up to date
        
        Configure GCS paths in `dags/utils/sync_manifests_from_gcs.py`.
        """,
    )

    sync_task

