import os
import requests
from config.settings import settings

def sync_file(local_path, target_path):
    print(f"Reading local file: {local_path}")
    
    if not os.path.exists(local_path):
        print(f"ERROR: Local file {local_path} does not exist.")
        return

    with open(local_path, "rb") as f:
        file_bytes = f.read()

    url = f"https://{settings.databricks_host}/api/2.0/fs/files{target_path}"

    print(f"Uploading to Databricks volume: {target_path}")
    resp = requests.put(
        url,
        headers={
            "Authorization": f"Bearer {settings.databricks_token}",
            "Content-Type": "application/octet-stream"
        },
        data=file_bytes,
        params={"overwrite": "true"}
    )

    try:
        resp.raise_for_status()
        print(f"SUCCESS: Successfully synced {local_path} to Databricks!\n")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response Body: {resp.text}")
        raise

if __name__ == "__main__":
    print("Starting Databricks Script Sync...")
    print("-" * 50)
    
    # 1. Sync run_query.py
    sync_file(
        local_path="databricks_scripts/databricks/run_query.py",
        target_path="/Volumes/inaas_dev/raw_analytics/raw_data/jobs/run_query.py"
    )
    
    # You can add more scripts to sync here in the future
    
    print("Sync complete!")
