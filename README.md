# dbt_repo_aggregator

Run multiple dbt projects in Apache Airflow (3.x) on Kubernetes using Astronomer Cosmos and the Airflow Helm chart. DAGs are synced from the `cosmos` branch via gitSync.

## Quick start
```bash
cd dbt_repo_aggregator/

# Build the custom Airflow image with dbt + Cosmos baked in
docker build -t poc/airflow-cosmos:3.0.6 .

# If using Colima without a registry: load the image into k8s containerd
docker save poc/airflow-cosmos:3.0.6 | colima nerdctl --namespace k8s.io load

# Install/upgrade Airflow with Helm (uses airflow-values.yaml)
helm repo add apache-airflow https://airflow.apache.org
helm repo update
helm upgrade --install airflow apache-airflow/airflow \
  --namespace airflow \
  --version 1.18.0 \
  -f airflow-values.yaml \
  --create-namespace \
  --timeout 15m

# Deploy postgres Db for testing models
kubectl apply -f postgres-deployment.yaml -n airflow

# Watch pods
kubectl get pods -n airflow --watch
```

## Access
```bash
# Airflow UI (webserver service name may vary by chart version)
kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow

# Optional: Postgres port-forward
kubectl port-forward -n airflow svc/postgres-service 5432:5432
```

## Connections
`postgres_dbt` is configured via values. To create/update manually:
```bash
kubectl exec -it -n airflow \
  $(kubectl get pods -n airflow -l component=scheduler -o name) \
  -- airflow connections add postgres_dbt \
  --conn-type postgres \
  --conn-host postgres-service \
  --conn-login dbt \
  --conn-password dbt \
  --conn-schema analytics \
  --conn-port 5432
```

## DAGs and dbt projects sync
- gitSync pulls this repo’s `cosmos` branch `dags/` into `/opt/airflow/dags` in pods.
- Push changes to GitHub to trigger sync (default ~10s interval).
- Airflow re-parses DAGs roughly every 10s.

### Git submodules
gitSync does not fetch submodules by default. If you modify a submodule:
1) Push the submodule repo changes
2) Update the super‑repo to the new submodule commit and push
3) If supported by your chart, enable `dags.gitSync.submodules: true` (or `recursive`)
Otherwise vendor the code (git subtree) or run `git submodule update --init --recursive` via an initContainer on the git volume.

## Example DAG
- DAG id: `run_multiple_dbt_projects_cosmos`
- Two dbt projects: `dags/dbt_repo_1` and `dags/dbt_repo_2`
- Uses Cosmos `DbtTaskGroup` with Postgres profile mapping

## Troubleshooting
1) Pods stuck in Init → wait for migrations to complete.
```bash
kubectl wait -n airflow --for=condition=complete job/airflow-run-airflow-migrations --timeout=300s
kubectl logs -n airflow job/airflow-run-airflow-migrations --tail=200
```

2) Image issues → ensure the custom image is available and values reference it:
- `images.airflow.repository: poc/airflow-cosmos`
- `images.airflow.tag: "3.0.6"`
- `images.airflow.pullPolicy: IfNotPresent`

3) Worker logs → fetched via K8s API. Ensure in values:
```
airflow.config.kubernetes.worker_pods_log_fetch_method: "pod"
airflow.config.kubernetes.enable_task_log_server: "False"
```

4) dbt task failing → get the worker pod logs for the dbt error:
```bash
kubectl get pods -n airflow | grep run-multiple-dbt-projects
kubectl logs -n airflow <pod-name> -c base --tail=500
```

## Cleanup (dev)
```bash
helm uninstall airflow -n airflow
kubectl delete namespace airflow
```