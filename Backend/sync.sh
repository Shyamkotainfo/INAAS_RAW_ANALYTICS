#!/bin/bash
# Description: Sync local python scripts to Databricks volumes

# Change to the Backend directory (where this script lives)
cd "$(dirname "$0")"

# Export the Python Path so it can find the config modules
export PYTHONPATH=$(pwd)

echo "Loading Environment and syncing to Databricks..."
python3 databricks_scripts/sync_scripts.py
