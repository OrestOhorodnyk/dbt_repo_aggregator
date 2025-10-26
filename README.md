# dbt_repo_aggregator

## set the release-name & namespace
export AIRFLOW_NAME="airflow"
export AIRFLOW_NAMESPACE="airflow"
## create the namespace
kubectl create ns "$AIRFLOW_NAMESPACE"
## install using helm 3
helm install \
  airflow \
  airflow-stable/airflow \
  --namespace airflow-cluster\
  --version "8.X.X" \
  --values ./values.yaml\
  --create-namespace

kubectl get pods -n airflow --watch 

helm uninstall airflow -n airflow
kubectl delete namespace airflow   
kubectl get pods -n airflow

kubectl port-forward svc/airflow-api-server 8080:8080 --namespace airflow


helm install airflow apache-airflow/airflow \
  --namespace airflow \
   --version 1.18.0  \
  -f airflow-values.yaml \
  --create-namespace \
  --timeout 15m

— add connetion
kubectl exec -it -n airflow deploy/airflow-scheduler -c scheduler -- \
airflow connections add 'kubernetes_default' \
  --conn-type 'kubernetes' \
  --conn-extra '{"in_cluster": true, "namespace": "airflow"}'

kubectl exec -n airflow deploy/airflow-scheduler -c scheduler -- \
airflow connections list



kubectl apply -f postgres-deployment.yaml -n airflow


docker build -t poc/dbt-orders:1.0.0 .
docker build -t poc/dbt-sales:1.0.0 .


docker run --rm \
  -e DBT_HOST=host.docker.internal \
  -e DBT_USER=dbt \
  -e DBT_PASS=dbt \
  -e DBT_DBNAME=analytics \
  -e DBT_SCHEMA=public \
  poc/dbt-sales:1.0.0