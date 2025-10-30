#!/bin/bash

# Cleanup and fresh deployment script for Apache Airflow
# This will delete the namespace and redeploy from scratch

set -e

NAMESPACE="airflow"
RELEASE_NAME="airflow"
VALUES_FILE="/Users/oohor/Documents/airflow on k8s/dbt_repo_aggregator/airflow-values.yaml"
HELM_CHART_VERSION="1.18.0"

echo "⚠️  WARNING: This will DELETE the entire Airflow deployment and namespace!"
echo "All data, DAGs, and configurations will be removed."
echo ""
read -p "Are you sure you want to continue? (yes/no): " -r
echo

if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Aborted."
    exit 1
fi

echo "🗑️  Uninstalling Airflow Helm release..."
helm uninstall "$RELEASE_NAME" -n "$NAMESPACE" 2>/dev/null || echo "No existing release found"

echo "🗑️  Deleting namespace '$NAMESPACE'..."
kubectl delete namespace "$NAMESPACE" --wait=false 2>/dev/null || echo "Namespace doesn't exist or already deleted"

echo "⏳ Waiting for namespace to be fully deleted..."
sleep 10

echo "📦 Updating Helm repositories..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || echo "Repository already exists"
helm repo update

echo "🔧 Creating namespace and deploying Airflow..."
helm install "$RELEASE_NAME" apache-airflow/airflow \
  --namespace "$NAMESPACE" \
  --version "$HELM_CHART_VERSION" \
  -f "$VALUES_FILE" \
  --create-namespace \
  --timeout 15m

echo ""
echo "✅ Deployment completed!"
echo ""
echo "To check the status, run:"
echo "  kubectl get pods -n $NAMESPACE"
echo ""
echo "To access the Airflow UI, port-forward the webserver:"
echo "  kubectl port-forward -n $NAMESPACE svc/$RELEASE_NAME-webserver 8080:8080"
echo ""
echo "Then open http://localhost:8080 in your browser"
echo ""
echo "📝 Note: Make sure your git repository has the fixed DAG file (without retries in ExecutionConfig)"
echo "   before gitSync pulls the code, otherwise you'll still see the error."

