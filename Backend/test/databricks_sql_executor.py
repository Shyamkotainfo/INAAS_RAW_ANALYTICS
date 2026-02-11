import requests
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class DatabricksSQLExecutor:
    def __init__(self):
        self.host = settings.databricks_host.rstrip("/")

        # Defensive: auto-fix missing scheme
        if not self.host.startswith("http"):
            self.host = f"https://{self.host}"

        self.endpoint = f"{self.host}/api/2.0/sql/statements"
        self.headers = {
            "Authorization": f"Bearer {settings.databricks_token}",
            "Content-Type": "application/json"
        }

    def execute(self, context: dict, sql_query: str):
        logger.info("Executing query using Databricks SQL")

        payload = {
            "statement": sql_query,
            "warehouse_id": settings.databricks_sql_warehouse_id,
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL"
        }

        response = requests.post(
            self.endpoint,
            headers=self.headers,
            json=payload,
            timeout=60
        )

        response.raise_for_status()
        return response.json()
