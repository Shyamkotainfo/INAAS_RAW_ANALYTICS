import os
import base64
import sys
import requests
import base64

CURRENT_DIR = os.path.dirname(__file__)
BACKEND_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, ".."))
if BACKEND_ROOT not in sys.path:
    sys.path.insert(0, BACKEND_ROOT)

from config.settings import settings

SCRIPT_ROOT = os.path.dirname(os.path.abspath(__file__))


def _resolve_local_path(local_path):
    if os.path.isabs(local_path):
        return local_path
    return os.path.normpath(os.path.join(SCRIPT_ROOT, "..", local_path))


def sync_file_to_volume(local_path, target_path):
    local_path = _resolve_local_path(local_path)
    print(f"Reading local file: {local_path}")
    
    if not os.path.exists(local_path):
        print(f"ERROR: Local file {local_path} does not exist.")
        return

    with open(local_path, "rb") as f:
        file_bytes = f.read()

    headers = {
        "Authorization": f"Bearer {settings.databricks_token}",
    }

    if target_path.startswith("dbfs:/"):
        url = f"https://{settings.databricks_host}/api/2.0/dbfs/put"
        payload = {
            "path": target_path,
            "overwrite": True,
            "contents": base64.b64encode(file_bytes).decode("ascii"),
        }
        print(f"Uploading to Databricks DBFS: {target_path}")
        resp = requests.post(url, headers=headers, json=payload)
    else:
        url = f"https://{settings.databricks_host}/api/2.0/fs/files{target_path}"
        print(f"Uploading to Databricks volume: {target_path}")
        resp = requests.put(
            url,
            headers={
                **headers,
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


def sync_file_to_dbfs(local_path, target_path):
    local_path = _resolve_local_path(local_path)
    print(f"Reading local file: {local_path}")

    if not os.path.exists(local_path):
        print(f"ERROR: Local file {local_path} does not exist.")
        return

    with open(local_path, "rb") as f:
        file_bytes = f.read()

    encoded = base64.b64encode(file_bytes).decode("utf-8")
    url = f"https://{settings.databricks_host}/api/2.0/dbfs/put"

    print(f"Uploading to DBFS path: {target_path}")
    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {settings.databricks_token}",
            "Content-Type": "application/json"
        },
        json={
            "path": target_path.replace("dbfs:", ""),
            "contents": encoded,
            "overwrite": True
        }
    )

    try:
        resp.raise_for_status()
        print(f"SUCCESS: Successfully synced {local_path} to Databricks!\n")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response Body: {resp.text}")
        raise


def sync_databricks_script(local_path, target_path):
    if target_path.startswith("dbfs:/") and not target_path.startswith("dbfs:/Volumes/"):
        sync_file_to_dbfs(local_path, target_path)
        return

    if target_path.startswith("dbfs:/Volumes/"):
        sync_file_to_volume(local_path, target_path.replace("dbfs:", "", 1))
        return

    sync_file_to_volume(local_path, target_path)

if __name__ == "__main__":
    print("Starting Databricks Script Sync...")
    print("-" * 50)

    # 1. Sync run_query.py
    sync_databricks_script(
        local_path="databricks_scripts/databricks/run_query.py",
        target_path=settings.databricks_run_query_script
    )

    # 2. Sync ingest_and_profile.py
    sync_databricks_script(
        local_path="databricks_scripts/databricks/ingest_and_profile.py",
        target_path=settings.databricks_ingest_script
    )

    print("Sync complete!")
