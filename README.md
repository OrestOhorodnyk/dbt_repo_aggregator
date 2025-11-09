# Apache Airflow on Kubernetes with dbt

Run Apache Airflow 3.0.6 on a local Kubernetes cluster (Colima + k3s) with Astronomer Cosmos orchestrating multiple dbt projects. Airflow itself uses `LocalExecutor`, while dbt models execute inside dedicated Kubernetes pods via `KubernetesPodOperator`. A scheduler sidecar keeps dbt repositories up to date, compiles them, and refreshes manifests so Cosmos can render the DAG structure at parse time.

## Prerequisites

- **Kubernetes**: Local cluster such as Colima/k3s, kind, or minikube
- **Helm 3**
- **kubectl**
- **Docker** (build and load custom Airflow/dbt images)
- **PostgreSQL**: Deployed through `postgres-deployment.yaml`

## Quick Start

1. **Sync host paths**
   ```bash
   ./update-paths.sh
   ```
   The script rewrites every `hostPath` entry in `airflow-values.yaml` so the Kubernetes Pods point to your local workspace.

2. **Bootstrap Helm**
   ```bash
   helm repo add apache-airflow https://airflow.apache.org
   helm repo update
   ```

3. **Deploy (automated)**
   ```bash
   ./deploy_airflow.sh
   ```
   The helper script installs the Helm chart, applies the PostgreSQL manifest, waits for the scheduler, and ensures the `postgres_dbt` connection exists.

   > Prefer a manual install? Re-run the Helm command printed by the script or use the sample below:
   >
   > ```bash
   > helm install airflow apache-airflow/airflow \
   >   --namespace airflow \
   >   --version 1.18.0 \
   >   -f airflow-values.yaml \
   >   --create-namespace \
   >   --timeout 15m
   >
   > kubectl apply -f postgres-deployment.yaml -n airflow
   > ```

4. **Watch pods**
   ```bash
   kubectl get pods -n airflow --watch
   ```
   You should eventually see `airflow-web`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `airflow-redis-master-0`, `airflow-pgbouncer`, and the standalone `postgres-*` instance. With `LocalExecutor` there is no Celery worker statefulset.

5. **Log in**
   ```bash
   kubectl port-forward -n airflow svc/airflow-web 8080:8080
   ```
   Open http://localhost:8080 and retrieve the password:
   ```bash
   kubectl get secret -n airflow airflow -o jsonpath='{.data.password}' | base64 -d && echo
   ```

## What the Deployment Does

- Mounts your local `dags/` and `airflow_logs/` directories into every Airflow component using `hostPath` volumes (ideal for local dev with Colima).
- Runs a scheduler sidecar (`dbt-git-sync`) that:
  - Clones `dbt_repo_1` and `dbt_repo_2` from GitHub (adjust in `airflow-values.yaml` to point to your own forks).
  - Runs `dbt deps`, `dbt clean`, and `dbt compile` when new commits arrive.
  - Copies `manifest.json` files into `/opt/airflow/dags/manifests/` and touches the primary DAG file so the parser notices changes.
- Configures essential Airflow connections (`postgres_dbt`, `kubernetes_default`, `google_cloud_default`).
- Uses `LocalExecutor`, leaving Cosmos free to spin up bespoke Kubernetes pods for dbt tasks.

## Project Layout

```
.
├── airflow-values.yaml         # Helm overrides for the Airflow chart
├── deploy_airflow.sh           # One-click deploy script (installs chart + DB)
├── postgres-deployment.yaml    # Dedicated Postgres for the dbt models
├── update-paths.sh             # Normalises hostPath entries to your workspace
├── dags/
│   ├── multiple_db_repos_with_extra_containers.py
│   ├── run_multiple_db_repos_cosmos_k8s.py
│   └── run_multiple_db_repos_with_extra_containers.py
├── Dockerfile                  # Base image used for the custom Airflow build
├── sync_requirements.txt       # Optional deps for out-of-cluster manifest sync
├── view-pod-logs.sh            # Helper to tail logs from Kubernetes pods
└── README.md
```

## Airflow Configuration Highlights

- **Airflow 3.0.6**, Helm chart `1.18.0`
- **Executor**: `LocalExecutor`
- **Base logs**: Mounted at `/opt/airflow/local_logs`, pointing to `<workspace>/airflow_logs`
- **HostPath volumes**: All components mount the same `dags` and `local_logs` directories so changes are instantly reflected
- **Connections provisioned by Helm**:
  - `postgres_dbt` → `postgres-service` (analytics DB for dbt)
  - `kubernetes_default` → in-cluster config (`namespace=airflow`)
  - `google_cloud_default` → optional GCP key located at `/opt/airflow/dags/utils/gcp-key.json`
- **Scheduler sidecar**: `dbt-git-sync` container handles git pulls and dbt compilation

## dbt DAGs

### `multiple_db_repos_with_extra_containers`

- Reads the dbt repositories cloned by the scheduler sidecar from `/opt/airflow/dags/dbt_repo_*/…`.
- Uses Cosmos `DbtTaskGroup` to render tasks directly from the project sources.
- Validates access to PostgreSQL (`test_postgres_connection`) before executing grouped tasks.
- Ideal when the scheduler and dag-processor share the same volume containing the repos.

### `run_dbt_projects_from_docker_images`

- Uses pre-built manifests and custom dbt Docker images (`poc/dbt-orders:1.0.3`, `poc/dbt-sales:1.0.1`).
- Each dbt task runs in a `KubernetesPodOperator` pod with its respective image.
- Relies on manifests generated by the sidecar and written to `/opt/airflow/dags/manifests/<repo>_manifest.json`.
- Demonstrates the Cosmos Kubernetes execution flow (manifests parsed at DAG time, tasks run in isolated pods).

> Both DAGs assume PostgreSQL credentials are supplied via the `postgres_dbt` Airflow connection. The dbt images’ `entrypoint.sh` recreates `profiles.yml` on the fly so Cosmos can locate the profile folder.

## Customising the Scheduler Sidecar

Locate the `scheduler.extraContainers[0]` block in `airflow-values.yaml` to tweak behaviour:

- Repositories: change the `for repo in dbt_repo_1 dbt_repo_2` loop or point the `git clone` URLs at your own projects.
- Sync interval: adjust `sleep 60`.
- dbt behaviour: modify the commands inside the loop (additional targets, tests, etc.).
- Touch file: update `touch /opt/airflow/dags/multiple_db_repos_with_extra_containers.py` if your primary DAG lives elsewhere.

## Building Images

```bash
# Airflow image with Cosmos/dbt baked in
docker build -t poc/airflow-cosmos:3.0.6 -f Dockerfile .

# Example dbt image
docker build -t poc/dbt-orders:1.0.3 -f Dockerfile.dbt-postgres .

# Load into Colima if needed
docker save poc/dbt-orders:1.0.3 | colima image load
```

Ensure the dbt image bundles an entrypoint that:
- Creates the `DBT_PROFILES_DIR` if missing
- Writes `profiles.yml` dynamically using environment variables
- Executes `dbt` with the arguments supplied by Cosmos

## Upgrading / Redeploying

- Simple upgrade:
  ```bash
  helm upgrade airflow apache-airflow/airflow \
    --namespace airflow \
    --version 1.18.0 \
    -f airflow-values.yaml \
    --timeout 15m
  ```
- Structural chart changes (e.g., volume name updates) can lead to patch conflicts. If you see `invalid patch format of retainKeys`, delete the affected deployment and re-run the upgrade:
  ```bash
  kubectl delete deployment -n airflow airflow-dag-processor
  helm upgrade airflow apache-airflow/airflow --namespace airflow \
    --version 1.18.0 -f airflow-values.yaml
  ```
- To remove everything:
  ```bash
  helm uninstall airflow -n airflow
  kubectl delete namespace airflow
  ```

## Troubleshooting

- **Pods pending or failing**
  ```bash
  kubectl get pods -n airflow
  kubectl describe pod <pod> -n airflow
  ```
- **Scheduler/DAG processor cannot see dbt repos**
  ```bash
  kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- ls -R /opt/airflow/dags | head
  kubectl exec -n airflow airflow-dag-processor-0 -c dag-processor -- ls /opt/airflow/dags/manifests
  ```
- **Manifest missing**
  ```bash
  kubectl exec -n airflow airflow-scheduler-0 -c dbt-git-sync -- \
    find /opt/airflow/dags -name manifest.json
  ```
- **Database connection**
  ```bash
  kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
    airflow connections test postgres_dbt
  ```
- **Log access**
  ```bash
  ./view-pod-logs.sh airflow-scheduler-0 scheduler
  ```
- **Path mismatches**
  ```bash
  ./update-paths.sh
  ```

## Useful Commands

- Airflow CLI shell:
  ```bash
  kubectl exec -it -n airflow airflow-scheduler-0 -c scheduler -- bash
  ```
- List DAGs:
  ```bash
  kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- airflow dags list
  ```
- Trigger DAG:
  ```bash
  kubectl exec -n airflow airflow-scheduler-0 -c scheduler -- \
    airflow dags trigger multiple_db_repos_with_extra_containers
  ```
- Inspect dbt pod logs (after a run):
  ```bash
  kubectl logs -n airflow <dbt-task-pod-name>
  ```

## Notes

- Designed for local experimentation; swap `hostPath` with PVCs/GitSync before production usage.
- `airflow_logs/` persists all scheduler/task logs on your workstation.
- `airflow-values.yaml` defaults to repositories hosted under `github.com/OrestOhorodnyk/`. Update URLs to match your org.
- The `google_cloud_default` connection is pre-created for future GCS integration; remove or adjust if unused.

## References

- [Apache Airflow](https://airflow.apache.org/docs/)
- [Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/)
- [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)
- [dbt Core](https://docs.getdbt.com/)
