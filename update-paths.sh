#!/bin/bash
# Helper script to update paths in airflow-values.yaml based on current workspace
# Usage: ./update-paths.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VALUES_FILE="$SCRIPT_DIR/airflow-values.yaml"

if [ ! -f "$VALUES_FILE" ]; then
    echo "Error: $VALUES_FILE not found"
    exit 1
fi

# Get the workspace directory (parent of dags folder)
WORKSPACE_DIR="$SCRIPT_DIR"
DAGS_PATH="$WORKSPACE_DIR/dags"
LOGS_PATH="$WORKSPACE_DIR/airflow_logs"

echo "Updating paths in $VALUES_FILE"
echo "Workspace: $WORKSPACE_DIR"
echo "DAGs path: $DAGS_PATH"
echo "Logs path: $LOGS_PATH"

# Use sed to replace paths (works on macOS and Linux)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    sed -i '' "s|path: \".*\/dags\"|path: \"$DAGS_PATH\"|g" "$VALUES_FILE"
    sed -i '' "s|path: \".*\/airflow_logs\"|path: \"$LOGS_PATH\"|g" "$VALUES_FILE"
    sed -i '' "s|# Current workspace: .*|# Current workspace: $WORKSPACE_DIR|g" "$VALUES_FILE"
else
    # Linux
    sed -i "s|path: \".*\/dags\"|path: \"$DAGS_PATH\"|g" "$VALUES_FILE"
    sed -i "s|path: \".*\/airflow_logs\"|path: \"$LOGS_PATH\"|g" "$VALUES_FILE"
    sed -i "s|# Current workspace: .*|# Current workspace: $WORKSPACE_DIR|g" "$VALUES_FILE"
fi

echo "✅ Paths updated successfully!"
echo ""
echo "Updated paths:"
grep -E "(path:|Current workspace:)" "$VALUES_FILE" | head -3

