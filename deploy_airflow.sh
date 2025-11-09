#!/bin/bash

# --- Configuration Variables ---
AIRFLOW_NS="airflow"
HELM_VERSION="1.18.0"
VALUES_FILE="airflow-values.yaml"
POSTGRES_FILE="postgres-deployment.yaml"
TIMEOUT_HELM="15m"
TIMEOUT_WAIT="300s" # 5 minutes for pods to be ready

echo "Starting Airflow and Database deployment..."

# 1. Install Airflow using Helm 3
# The --create-namespace flag handles namespace creation.
echo "## 🚀 Step 1: Installing Airflow Helm Chart..."
helm install airflow apache-airflow/airflow \
  --namespace "${AIRFLOW_NS}" \
  --version "${HELM_VERSION}" \
  -f "${VALUES_FILE}" \
  --create-namespace \
  --timeout "${TIMEOUT_HELM}"
if [ $? -ne 0 ]; then
    echo "❌ Helm installation failed. Exiting."
    exit 1
fi
echo "Airflow deployment initiated."

# 2. Deploy PostgreSQL
echo "## 💾 Step 2: Applying PostgreSQL deployment..."
kubectl apply -f "${POSTGRES_FILE}" -n "${AIRFLOW_NS}"
if [ $? -ne 0 ]; then
    echo "❌ kubectl apply failed for PostgreSQL. Exiting."
    exit 1
fi
echo "PostgreSQL deployment initiated."

# 3. Wait for Airflow Scheduler Pod to be Ready
# This is crucial, as the next command depends on the scheduler being fully running.
echo "## ⏳ Step 3: Waiting for Airflow Scheduler pod to be Ready (up to ${TIMEOUT_WAIT})..."
kubectl wait --for=condition=ready pod -l component=scheduler -n "${AIRFLOW_NS}" --timeout="${TIMEOUT_WAIT}"
if [ $? -ne 0 ]; then
    echo "❌ Airflow Scheduler pod did not become ready in time. Check 'kubectl get pods -n ${AIRFLOW_NS}'. Exiting."
    exit 1
fi
echo "Airflow Scheduler is Ready."

# 4. Execute the Airflow Connection Command
# Finds the exact scheduler pod name and executes the connections command inside it.
SCHEDULER_POD=$(kubectl get pods -n "${AIRFLOW_NS}" -l component=scheduler -o name)
if [ -z "${SCHEDULER_POD}" ]; then
    echo "❌ Could not find the Airflow Scheduler pod name. Exiting."
    exit 1
fi

echo "## ⚙️ Step 4: Adding 'postgres_dbt' connection inside pod: ${SCHEDULER_POD}..."
kubectl exec -it -n "${AIRFLOW_NS}" "${SCHEDULER_POD}" -- \
  airflow connections add postgres_dbt \
  --conn-type postgres \
  --conn-host postgres-service \
  --conn-login dbt \
  --conn-password dbt \
  --conn-schema analytics \
  --conn-port 5432
if [ $? -ne 0 ]; then
    echo "❌ Failed to add Airflow connection."
    exit 1
fi

echo "✅ All steps completed successfully!"