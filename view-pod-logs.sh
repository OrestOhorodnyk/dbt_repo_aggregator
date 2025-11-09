sh#!/bin/bash
# Helper script to view logs from dbt pods
# Usage: ./view-pod-logs.sh [pod-name-pattern]

NAMESPACE="airflow"
PATTERN="${1:-dbt-repo}"

echo "Finding pods matching pattern: $PATTERN"
echo ""

# List matching pods
PODS=$(kubectl get pods -n "$NAMESPACE" | grep "$PATTERN" | awk '{print $1}')

if [ -z "$PODS" ]; then
    echo "No pods found matching pattern: $PATTERN"
    echo ""
    echo "All pods in namespace $NAMESPACE:"
    kubectl get pods -n "$NAMESPACE"
    exit 1
fi

echo "Found pods:"
echo "$PODS"
echo ""

# Show logs for each pod
for POD in $PODS; do
    echo "=========================================="
    echo "Logs for pod: $POD"
    echo "=========================================="
    kubectl logs -n "$NAMESPACE" "$POD" --tail=100
    echo ""
    echo ""
done

# Also show pod status
echo "=========================================="
echo "Pod status:"
echo "=========================================="
kubectl get pods -n "$NAMESPACE" | grep "$PATTERN"

