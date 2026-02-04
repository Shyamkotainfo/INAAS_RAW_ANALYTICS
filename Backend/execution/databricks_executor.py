import requests
from config.settings import settings


class DatabricksExecutor:
    def execute(self, file_path: str, pyspark_code: str):
        payload = {
            "run_name": "INAAS Raw Query",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": "dbfs:/inaas/jobs/run_query.py",
                "parameters": [file_path, pyspark_code]
            }
        }

        response = requests.post(
            f"{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={
                "Authorization": f"Bearer {settings.databricks_token}"
            },
            json=payload
        )

        response.raise_for_status()
        return response.json()
