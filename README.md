# Apache Airflow on Kubernetes with dbt

This project deploys Apache Airflow 3.0.6 on Kubernetes (using Helm) with support for running dbt projects using Astronomer Cosmos. The setup uses KubernetesExecutor and includes configuration for local development with Colima/k3s.

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
├── airflow-values.yaml      # Helm values for Airflow deployment
├── postgres-deployment.yaml  # PostgreSQL deployment for dbt
├── dags/                     # Airflow DAGs directory
│   ├── run_multiple_db_repos.py  # Main DAG for running dbt projects
│   ├── dbt_repo_1/          # First dbt project
│   └── dbt_repo_2/          # Second dbt project
├── update-paths.sh          # Script to update paths dynamically
└── README.md                # This file
```

## Configuration

### Airflow Version
- **Airflow**: 3.0.6
- **Helm Chart**: 1.18.0
- **Executor**: KubernetesExecutor

### Key Features
- ✅ Custom Airflow image with Cosmos and dbt pre-installed
- ✅ HostPath volumes for DAGs and logs (local development)
- ✅ KubernetesExecutor for scalable task execution
- ✅ GitSync disabled (using local volume mounts)
- ✅ PostgreSQL connections configured for dbt projects

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

## DAGs

### Main DAG: `run_multiple_db_repos`

This DAG dynamically runs dbt projects using KubernetesPodOperator:

1. **Test PostgreSQL Connection**: Validates connectivity to the dbt database
2. **dbt Seed**: Runs `dbt seed` for each project
3. **dbt Run**: Runs `dbt run` for each project

**dbt Images:**
- `poc/dbt-orders:1.0.0` (for dbt_repo_1)
- `poc/dbt-sales:1.0.0` (for dbt_repo_2)

To build custom dbt images:
```bash
docker build -t poc/dbt-orders:1.0.0 -f Dockerfile.dbt-orders .
docker build -t poc/dbt-sales:1.0.0 -f Dockerfile.dbt-sales .
```

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
kubectl logs -n airflow deploy/airflow-dag-processor -c dag-processor --tail=50

# Verify DAGs are mounted correctly
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- ls -la /opt/airflow/dags
```

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
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
  airflow version
```

### List Connections
```bash
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
  airflow connections list
```

### View Scheduler Logs
```bash
kubectl logs -n airflow deploy/airflow-scheduler -c scheduler --tail=100 -f
```

### Access Airflow CLI
```bash
kubectl exec -it -n airflow deploy/airflow-scheduler -c scheduler -- bash
# Then run: airflow <command>
```

### Check DAG Status
```bash
kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
  airflow dags list
```

## Notes

- **Local Development**: This setup uses `hostPath` volumes for local development with Colima/k3s. For production, consider using PersistentVolumeClaims or GitSync.
- **Logs**: Logs are stored in `<workspace>/airflow_logs` directory on the host.
- **DAGs**: DAGs are synced from `<workspace>/dags` directory.
- **Image Repository**: The default image `poc/airflow-cosmos:3.0.6` should be built and available. See your Docker setup for building this image.

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Airflow Helm Chart](https://airflow.apache.org/docs/helm-chart/stable/index.html)
- [Astronomer Cosmos](https://github.com/astronomer/astronomer-cosmos)
- [dbt Documentation](https://docs.getdbt.com/)
