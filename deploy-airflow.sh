#!/bin/bash

# Deployment script for Apache Airflow on Kubernetes
# Uses the airflow-values.yaml file

set -e

NAMESPACE="airflow"
RELEASE_NAME="airflow"
VALUES_FILE="/Users/oohor/Documents/airflow on k8s/dbt_repo_aggregator/airflow-values.yaml"
HELM_REPO_URL="https://airflow.apache.org"

echo "🚀 Starting Airflow deployment..."

# Add Apache Airflow Helm repository if not already added
echo "📦 Adding Apache Airflow Helm repository..."
helm repo add apache-airflow "$HELM_REPO_URL" 2>/dev/null || echo "Repository already exists"
helm repo update

# Create namespace if it doesn't exist
echo "📁 Creating namespace '$NAMESPACE' if it doesn't exist..."
kubectl create namespace "$NAMESPACE" 2>/dev/null || echo "Namespace '$NAMESPACE' already exists"

# Deploy/Upgrade Airflow
echo "🔧 Deploying Airflow with values from $VALUES_FILE..."
helm upgrade --install "$RELEASE_NAME" apache-airflow/airflow \
  --namespace "$NAMESPACE" \
  --values "$VALUES_FILE" \
  --wait

echo "✅ Airflow deployment completed!"
echo ""
echo "To check the status, run:"
echo "  kubectl get pods -n $NAMESPACE"
echo ""
echo "To access the Airflow UI, port-forward the webserver:"
echo "  kubectl port-forward -n $NAMESPACE svc/$RELEASE_NAME-webserver 8080:8080"
echo ""
echo "Then open http://localhost:8080 in your browser"
