# Backend/ingestion/databricks_ingestion_executor.py

import json
import time
import re
import boto3
import requests
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class DatabricksIngestionExecutor:

    def run(self):
        run_id = self._submit_job()
        stdout = self._wait_and_fetch_logs(run_id)
        schema = self._extract_schema(stdout)
        self._write_schema_to_s3(schema)
        self._trigger_bedrock_ingestion()

        return {"run_id": run_id, "status": "SUCCESS"}

    # -------------------------
    def _submit_job(self):
        payload = {
            "run_name": "INAAS - Schema Extraction",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_schema_extractor_script
            }
        }

        r = requests.post(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            json=payload
        )
        r.raise_for_status()
        run_id = r.json()["run_id"]
        logger.info(f"Databricks run_id: {run_id}")
        return run_id

    # -------------------------
    def _wait_and_fetch_logs(self, run_id):
        while True:
            r = requests.get(
                f"https://{settings.databricks_host}/api/2.1/jobs/runs/get",
                headers={"Authorization": f"Bearer {settings.databricks_token}"},
                params={"run_id": run_id}
            ).json()

            state = r["state"]["life_cycle_state"]
            logger.info(f"Run {run_id} | {state}")

            if state == "TERMINATED":
                break

            time.sleep(5)

        out = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        ).json()

        return out.get("logs", "")

    # -------------------------
    def _extract_schema(self, stdout: str):
        match = re.search(r"INAAS_SCHEMA_JSON=(\{.*\})", stdout)
        if not match:
            raise RuntimeError("Schema JSON not found in logs")

        return json.loads(match.group(1))

    # -------------------------
    def _write_schema_to_s3(self, schema: dict):
        s3 = boto3.client("s3", region_name=settings.aws_region)
        key = f"schema/{schema['file_id']}.json"

        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=json.dumps(schema, indent=2),
            ContentType="application/json"
        )

        logger.info(f"Schema written to s3://{settings.s3_bucket}/{key}")

    # -------------------------
    def _trigger_bedrock_ingestion(self):
        client = boto3.client("bedrock-agent", region_name=settings.aws_region)
        client.start_ingestion_job(
            knowledgeBaseId=settings.bedrock_kb_id,
            dataSourceId=settings.bedrock_data_source_id
        )

        logger.info("Bedrock ingestion triggered")
