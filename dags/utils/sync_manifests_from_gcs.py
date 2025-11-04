#!/usr/bin/env python3
"""
Sync dbt manifests from GCS bucket to local filesystem.

This script should be run:
1. Before DAG deployment (manually or via CI/CD)
2. Periodically via cron/scheduler to keep manifests updated
3. As part of your deployment pipeline

Usage:
    python sync_manifests_from_gcs.py

Authentication:
    - Uses gcp-key.json in project root (if exists)
    - Falls back to GOOGLE_APPLICATION_CREDENTIALS environment variable
    - Or use: gcloud auth application-default login
"""

import os
from pathlib import Path

# Try to import both - prefer google.cloud.storage when credentials are available
USE_AIRFLOW_HOOK = False
GCS_HOOK_AVAILABLE = False
STORAGE_CLIENT_AVAILABLE = False

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    GCS_HOOK_AVAILABLE = True
except ImportError:
    pass

try:
    from google.cloud import storage
    STORAGE_CLIENT_AVAILABLE = True
except ImportError:
    pass

if not GCS_HOOK_AVAILABLE and not STORAGE_CLIENT_AVAILABLE:
    raise ImportError(
        "Either install 'apache-airflow-providers-google' (for Airflow) "
        "or 'google-cloud-storage' (for standalone). "
        "Run: pip install google-cloud-storage"
    )

# Path to GCP service account key file
# Check multiple locations: utils directory, project root, or environment variable
GCP_KEY_FILE = None
for possible_path in [
    Path(__file__).parent / "gcp-key.json",  # dags/utils/gcp-key.json
    Path(__file__).parent.parent.parent / "gcp-key.json",  # project root
]:
    if possible_path.exists():
        GCP_KEY_FILE = possible_path
        break

# Configuration - matches DBT_IMAGES in the DAG
# NOTE: /opt/airflow/dags is read-only, so we write to shared local_logs volume
# This is accessible by all pods (scheduler, dagProcessor, etc.)
MANIFEST_CONFIGS = {
    "dbt_repo_1": {
        # Uncomment and set GCS path if using GCS:
        "gcs_path": "gs://dbt-manifest-for-cosmos/dbt_repo_1/target/manifest.json",
        # Write to shared local_logs volume (accessible by all pods)
        "local_path": "/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json",
        "temp_path": None,  # Not needed - using shared volume
    },
    "dbt_repo_2": {
        # Uncomment and set GCS path if using GCS:
        "gcs_path":  "gs://dbt-manifest-for-cosmos/dbt_repo_2/target/manifest.json",
        # Write to shared local_logs volume (accessible by all pods)
        "local_path": "/opt/airflow/local_logs/manifests/dbt_sales_stub/target/manifest.json",
        "temp_path": None,  # Not needed - using shared volume
    },
}


def setup_gcp_credentials():
    """Set up GCP credentials from gcp-key.json if available."""
    global USE_AIRFLOW_HOOK
    
    # Check if gcp-key.json file was found
    if GCP_KEY_FILE and GCP_KEY_FILE.exists():
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(GCP_KEY_FILE)
        print(f"✅ Using GCP credentials from: {GCP_KEY_FILE}")
        # When credentials are set via env var, prefer google.cloud.storage
        USE_AIRFLOW_HOOK = False
    elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        print(f"✅ Using GCP credentials from: {os.getenv('GOOGLE_APPLICATION_CREDENTIALS')}")
        USE_AIRFLOW_HOOK = False
    else:
        print("⚠️  No explicit credentials found - will use default credentials")
        print("   (gcloud auth application-default login or service account)")
        # If no explicit credentials, try using Airflow connection if available
        if GCS_HOOK_AVAILABLE:
            USE_AIRFLOW_HOOK = True


def download_manifest_from_gcs(gcs_path: str, local_path: str, temp_path: str = None) -> None:
    """
    Download manifest.json from GCS bucket to local path.
    
    Uses shared local_logs volume (accessible by all pods).
    """
    # Extract bucket and blob from gs:// path
    path_parts = gcs_path.replace("gs://", "").split("/", 1)
    if len(path_parts) != 2:
        raise ValueError(f"Invalid GCS path format: {gcs_path}")
    
    bucket_name = path_parts[0]
    blob_name = path_parts[1]
    
    # Use local_path directly (shared volume, writable)
    download_path = local_path
    download_file = Path(download_path)
    
    # Ensure directory exists
    download_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Download from GCS using either Airflow hook or Google Cloud Storage library
    if USE_AIRFLOW_HOOK and GCS_HOOK_AVAILABLE:
        # Use Airflow's GCSHook (if running in Airflow environment and no explicit credentials)
        gcs_hook = GCSHook()
        gcs_hook.download(
            bucket_name=bucket_name,
            object_name=blob_name,
            filename=str(download_file),
        )
    elif STORAGE_CLIENT_AVAILABLE:
        # Use Google Cloud Storage library directly (preferred when credentials are set)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.download_to_filename(str(download_file))
    else:
        raise RuntimeError("Neither GCSHook nor google.cloud.storage is available")
    
    print(f"✅ Downloaded manifest from {gcs_path} to {download_path}")


def sync_manifests(raise_on_failure=True):
    """
    Sync all manifests from GCS to local filesystem.
    
    Args:
        raise_on_failure: If True, raise exception on any sync failure. If False, return failure count.
    
    Returns:
        tuple: (synced_count, failed_count, failures)
    
    Raises:
        RuntimeError: If raise_on_failure=True and any sync fails
    """
    synced_count = 0
    failed_count = 0
    failures = []
    
    # Get repos that should be synced (have gcs_path configured)
    repos_to_sync = [
        (repo, config) for repo, config in MANIFEST_CONFIGS.items() 
        if config.get("gcs_path")
    ]
    
    if not repos_to_sync:
        print("⚠️  No GCS paths configured - nothing to sync")
        return 0, 0, []
    
    for repo, config in repos_to_sync:
        gcs_path = config.get("gcs_path")
        local_path = config["local_path"]
        
        try:
            download_manifest_from_gcs(gcs_path, local_path)
            synced_count += 1
        except Exception as e:
            failed_count += 1
            error_msg = f"Failed to sync {repo}: {e}"
            print(f"❌ {error_msg}")
            failures.append({"repo": repo, "error": str(e), "gcs_path": gcs_path})
    
    print(f"\n✅ Synced {synced_count} manifest(s) from GCS")
    if failed_count > 0:
        print(f"❌ Failed to sync {failed_count} manifest(s)")
    
    if raise_on_failure and failed_count > 0:
        error_summary = "; ".join([f"{f['repo']}: {f['error']}" for f in failures])
        raise RuntimeError(
            f"Failed to sync {failed_count} manifest(s): {error_summary}"
        )
    
    return synced_count, failed_count, failures


if __name__ == "__main__":
    # Set up GCP credentials before syncing
    setup_gcp_credentials()
    # When run standalone, raise on failure
    sync_manifests(raise_on_failure=True)

