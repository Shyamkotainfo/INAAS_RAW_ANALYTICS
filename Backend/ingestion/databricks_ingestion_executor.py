import requests
from config.settings import settings


class DatabricksIngestionExecutor:
    def run(self):
        payload = {
            "run_name": "INAAS Schema Extraction",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": "dbfs:/inaas/jobs/schema_extractor.py"
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
