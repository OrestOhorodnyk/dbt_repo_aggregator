# Apache Airflow on Kubernetes with dbt

This project deploys Apache Airflow 3.0.6 on Kubernetes (using Helm) with support for running dbt projects using Astronomer Cosmos. The setup uses LocalExecutor with KubernetesPodOperator for dbt tasks and includes configuration for local development with Colima/k3s. It also supports syncing dbt manifests from GCS buckets.

## Prerequisites

- **Kubernetes**: Local Kubernetes cluster (Colima, k3s, minikube, or similar)
- **Helm 3**: Package manager for Kubernetes
- **kubectl**: Kubernetes command-line tool
- **Docker**: For building custom dbt images (if needed)
- **PostgreSQL**: Available as a service in the cluster (deployed via `postgres-deployment.yaml`)

## Quick Start

### 1. Configure Paths

The `airflow-values.yaml` file uses dynamic paths based on your workspace directory. If you move the project or use it on a different machine:

**Option A: Automatic (Recommended)**
```bash
./update-paths.sh
```

**Option B: Manual**
Edit `airflow-values.yaml` and update all path references:
- Search for `path: "..."` entries
- Update to match your workspace location
- Paths should point to: `<workspace>/dags` and `<workspace>/airflow_logs`

### 2. Add Helm Repository

```bash
helm repo add apache-airflow https://airflow.apache.org
helm repo update
```

### 3. Create Namespace

```bash
kubectl create namespace airflow
```

### 4. Deploy Airflow

```bash
helm install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.18.0 \
  -f airflow-values.yaml \
  --create-namespace \
  --timeout 15m
```

### 5. Deploy PostgreSQL (for dbt)

```bash
kubectl apply -f postgres-deployment.yaml -n airflow
```

### 6. Monitor Deployment

```bash
kubectl get pods -n airflow --watch
```

Wait until all pods are in `Running` state:
- `airflow-web` (Webserver)
- `airflow-scheduler`
- `airflow-dag-processor`
- `airflow-triggerer`
- `airflow-worker`
- `airflow-postgresql-0`
- `airflow-redis-master-0`
- `airflow-pgbouncer`
- `postgres-*` (dbt database)

### 7. Access Airflow UI

```bash
# Port-forward the webserver service
kubectl port-forward -n airflow svc/airflow-web 8080:8080
```

Then open: http://localhost:8080

**Default Credentials:**
- Username: `admin`
- Password: Get from secret:
  ```bash
  kubectl get secret -n airflow airflow -o jsonpath='{.data.password}' | base64 -d && echo
  ```

## Project Structure

```
.
├── airflow-values.yaml              # Helm values for Airflow deployment
├── postgres-deployment.yaml          # PostgreSQL deployment for dbt
├── Dockerfile                         # Dockerfile for building dbt images
├── dags/                              # Airflow DAGs directory
│   ├── run_multiple_db_repos_cosmos_k8s.py  # Main DAG for running dbt projects
│   ├── sync_manifests_dag.py         # DAG to sync manifests from GCS
│   ├── utils/                         # Utility scripts
│   │   └── sync_manifests_from_gcs.py  # Script to download manifests from GCS
│   └── manifests/                     # Stub dbt projects for Cosmos
│       ├── dbt_orders_stub/
│       └── dbt_sales_stub/
├── gcp-key.json                      # GCP service account key (not in git)
├── sync_requirements.txt             # Python requirements for sync script
├── GCP_SETUP.md                      # GCP setup instructions
└── README.md                          # This file
```

## Configuration

### Airflow Version
- **Airflow**: 3.0.6
- **Helm Chart**: 1.18.0
- **Executor**: LocalExecutor (with KubernetesPodOperator for dbt tasks)
- **Cosmos**: 1.11.0

### Key Features
- ✅ Custom Airflow image with Cosmos and dbt pre-installed
- ✅ HostPath volumes for DAGs and logs (local development)
- ✅ LocalExecutor with KubernetesPodOperator for dbt task execution
- ✅ GitSync disabled (using local volume mounts)
- ✅ PostgreSQL connections configured for dbt projects
- ✅ GCS manifest sync support (syncs dbt manifests from GCS buckets)
- ✅ Automatic manifest syncing every 10 minutes

### Connections

The following connections are pre-configured in `airflow-values.yaml`:

**PostgreSQL (for dbt):**
- Connection ID: `postgres_dbt`
- Host: `postgres-service`
- Database: `analytics`
- User: `dbt`
- Password: `dbt`

**Kubernetes:**
- Connection ID: `kubernetes_default`
- Configured for in-cluster access

**Google Cloud Platform (for GCS manifest sync):**
- Connection ID: `google_cloud_default`
- Type: `google_cloud_platform`
- Keyfile path: `/opt/airflow/dags/utils/gcp-key.json`

## DAGs

### Main DAG: `run_dbt_projects_from_docker_images`

This DAG runs dbt projects using Astronomer Cosmos with KubernetesPodOperator:

- Uses Cosmos `DbtTaskGroup` to automatically generate dbt tasks from manifest files
- Each dbt project runs in its own Kubernetes pod with a custom Docker image
- Supports both local and GCS-synced manifest files

**dbt Images:**
- `poc/dbt-orders:1.0.3` (for dbt_repo_1)
- `poc/dbt-sales:1.0.1` (for dbt_repo_2)

**Manifest Paths:**
- Manifests are stored in `/opt/airflow/local_logs/manifests/` (shared volume)
- Paths:
  - `dbt_repo_1`: `/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json`
  - `dbt_repo_2`: `/opt/airflow/local_logs/manifests/dbt_sales_stub/target/manifest.json`

### Sync DAG: `sync_manifests_from_gcs`

This DAG automatically syncs dbt manifests from GCS buckets:

- **Schedule**: Every 10 minutes (`*/10 * * * *`)
- **Function**: Downloads manifest.json files from GCS to local filesystem
- **Location**: Writes to `/opt/airflow/local_logs/manifests/` (shared volume)
- **Configuration**: Edit `dags/utils/sync_manifests_from_gcs.py` to set GCS paths

**Setup GCS Sync:**

1. **Place GCP service account key:**
   ```bash
   cp /path/to/gcp-key.json dags/utils/gcp-key.json
   ```

2. **Configure GCS paths** in `dags/utils/sync_manifests_from_gcs.py`:
   ```python
   MANIFEST_CONFIGS = {
       "dbt_repo_1": {
           "gcs_path": "gs://your-bucket/manifests/dbt_repo_1/target/manifest.json",
           "local_path": "/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json",
       },
       # ...
   }
   ```

3. **The sync DAG will automatically run every 10 minutes**

**Important Notes:**
- Cosmos requires manifests at **DAG parse time** (when scheduler loads the DAG)
- Ensure the sync DAG runs successfully before the dbt DAG is first parsed
- Or manually trigger sync DAG before deploying dbt DAGs

See `GCP_SETUP.md` for detailed GCP setup instructions.

### Building dbt Images

To build custom dbt images:

```bash
# Build dbt image
docker build -t poc/dbt-orders:1.0.3 -f Dockerfile .

# Load into Colima (if using local registry)
docker save poc/dbt-orders:1.0.3 | colima image load
```

**Image Requirements:**
- Must have dbt and dbt-postgres installed
- Must include `entrypoint.sh` that forwards arguments to dbt
- Must create `profiles.yml` dynamically from environment variables
- See `Dockerfile` and `entrypoint.sh` for reference

## Troubleshooting

### Pods Not Starting

```bash
# Check pod status
kubectl get pods -n airflow --watch

# Check events
kubectl get events -n airflow --sort-by='.lastTimestamp'

# View pod logs
kubectl logs -n airflow <pod-name> -c <container-name>
```

### DAG Import Errors

```bash
# Check DAG processor logs
kubectl logs -n airflow airflow-dag-processor-<pod-id> -c dag-processor --tail=50

# Verify DAGs are mounted correctly
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- ls -la /opt/airflow/dags

# Check if manifests exist (for Cosmos DAGs)
kubectl exec -n airflow airflow-dag-processor-<pod-id> -c dag-processor -- \
  ls -la /opt/airflow/local_logs/manifests/

# Verify manifest files are valid JSON
kubectl exec -n airflow airflow-dag-processor-<pod-id> -c dag-processor -- \
  python3 -c "import json; f=open('/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json'); json.load(f); print('✅ Manifest is valid')"
```

**Common Issues:**
- **Manifest not found**: Ensure sync DAG has run successfully and manifests are in `/opt/airflow/local_logs/manifests/`
- **Read-only filesystem**: Manifests must be written to shared volume (`/opt/airflow/local_logs/`), not `/tmp`
- **Cosmos parse error**: Cosmos needs manifests at DAG parse time - trigger sync DAG manually before first dbt DAG run

### Database Connection Issues

```bash
# Test PostgreSQL connection
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
  airflow connections test postgres_dbt

# Check PostgreSQL pod
kubectl get pods -n airflow | grep postgres
kubectl logs -n airflow <postgres-pod-name>
```

### Path Issues

If paths are incorrect, update them:
```bash
./update-paths.sh
```

Or manually edit `airflow-values.yaml` and search for `path:` entries.

### Image Pull Errors

If using local images with Colima:
```bash
# Build and load image into Colima
docker build -t poc/airflow-cosmos:3.0.6 .
colima cp poc/airflow-cosmos:3.0.6 colima:/tmp/
# Then import into containerd (inside Colima)
```

Or use a container registry.

## Upgrading

```bash
helm upgrade airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.18.0 \
  -f airflow-values.yaml \
  --timeout 15m
```

## Uninstallation

```bash
# Uninstall Helm release
helm uninstall airflow -n airflow

# Delete namespace (removes all resources)
kubectl delete namespace airflow
```

**Note:** This will delete all data, including the Airflow metadata database.

## Useful Commands

### Check Airflow Version
```bash
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow version
```

### List Connections
```bash
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow connections list
```

### View Scheduler Logs
```bash
kubectl logs -n airflow airflow-scheduler-0 -c scheduler --tail=100 -f
```

### Access Airflow CLI
```bash
kubectl exec -it -n airflow airflow-scheduler-0 -c scheduler -- bash
# Then run: airflow <command>
```

### Check DAG Status
```bash
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow dags list
```

### Manually Sync Manifests from GCS
```bash
# Run sync script manually in scheduler pod
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  python /opt/airflow/dags/utils/sync_manifests_from_gcs.py

# Or trigger sync DAG from UI or CLI
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  airflow dags trigger sync_manifests_from_gcs
```

### Check Manifest Files
```bash
# List manifests in shared volume
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  find /opt/airflow/local_logs/manifests -name "manifest.json"

# Verify manifest is readable
kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
  ls -lh /opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json
```

## GCS Manifest Sync Setup

### Initial Setup

1. **Place GCP service account key:**
   ```bash
   cp /path/to/gcp-key.json dags/utils/gcp-key.json
   ```
   The key file is already in `.gitignore` and won't be committed.

2. **Configure GCS paths** in `dags/utils/sync_manifests_from_gcs.py`:
   ```python
   MANIFEST_CONFIGS = {
       "dbt_repo_1": {
           "gcs_path": "gs://your-bucket/manifests/dbt_repo_1/target/manifest.json",
           "local_path": "/opt/airflow/local_logs/manifests/dbt_orders_stub/target/manifest.json",
       },
       "dbt_repo_2": {
           "gcs_path": "gs://your-bucket/manifests/dbt_repo_2/target/manifest.json",
           "local_path": "/opt/airflow/local_logs/manifests/dbt_sales_stub/target/manifest.json",
       },
   }
   ```

3. **The sync DAG runs automatically** every 10 minutes, or trigger it manually from the Airflow UI.

### How It Works

- **Sync DAG** (`sync_manifests_from_gcs`): Downloads manifests from GCS every 10 minutes
- **Manifest Storage**: Writes to `/opt/airflow/local_logs/manifests/` (shared volume accessible by all pods)
- **dbt DAG**: Reads manifests from the shared location when parsing DAG structure
- **Important**: Cosmos validates manifests at DAG parse time, so ensure sync DAG runs before dbt DAG is first loaded

### Manual Sync

If you need to sync manually before deployment:

```bash
# Install dependencies locally (if needed)
pip install google-cloud-storage

# Run sync script
python dags/utils/sync_manifests_from_gcs.py
```

See `GCP_SETUP.md` for detailed GCP authentication setup.

## Notes

- **Local Development**: This setup uses `hostPath` volumes for local development with Colima/k3s. For production, consider using PersistentVolumeClaims or GitSync.
- **Logs**: Logs are stored in `<workspace>/airflow_logs` directory on the host.
- **Manifests**: Manifests are stored in `<workspace>/airflow_logs/manifests/` (shared volume).
- **DAGs**: DAGs are synced from `<workspace>/dags` directory.
- **Image Repository**: The default image `poc/airflow-cosmos:3.0.6` should be built and available. See your Docker setup for building this image.
- **Executor**: Using `LocalExecutor` instead of `KubernetesExecutor` to allow `KubernetesPodOperator` to create pods with custom images independently.
- **Manifest Storage**: Manifests are stored in shared `local_logs` volume so they're accessible by scheduler, dagProcessor, and other pods.

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)
- [dbt Documentation](https://docs.getdbt.com/)
