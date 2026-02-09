import requests
import time
import json
from config.settings import settings


class DatabricksExecutor:
    def execute(self, context: dict, pyspark_code: str):
        payload = {
            "run_name": "INAAS Query",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_run_query_script,
                "parameters": [
                    json.dumps({
                        "file_path": context["file_path"],
                        "format": context["format"],
                        "pyspark_code": pyspark_code
                    })
                ]
            }
        }

        r = requests.post(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            json=payload
        )
        r.raise_for_status()
        run_id = r.json()["run_id"]

        # Poll
        while True:
            s = requests.get(
                f"https://{settings.databricks_host}/api/2.1/jobs/runs/get",
                headers={"Authorization": f"Bearer {settings.databricks_token}"},
                params={"run_id": run_id}
            ).json()

            if s["state"]["life_cycle_state"] == "TERMINATED":
                break
            time.sleep(2)

        # Fetch output
        out = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        ).json()

        logs = out.get("logs", "")
        for line in reversed(logs.splitlines()):
            if line.startswith("INAAS_RESULT:"):
                return json.loads(line.replace("INAAS_RESULT:", "").strip())

        return {"status": "NO_RESULT"}
