# Backend/ingestion/databricks_ingestion_executor.py

import time
import json
import requests
from datetime import datetime, timezone

import boto3

from config.settings import settings
from logger.logger import get_logger
from rag.bedrock_ingestor import trigger_bedrock_ingestion

logger = get_logger(__name__)


class DatabricksIngestionExecutor:
    """
    Responsibilities:
    - Run schema_extractor.py on Databricks
    - Capture stdout
    - Extract schema JSON
    - Persist schema to S3
    - Trigger Bedrock KB ingestion
    """

    def run(self):
        self._validate()

        run_id = self._submit_job()
        stdout = self._wait_and_fetch_logs(run_id)

        schema = self._extract_schema(stdout)
        s3_uri = self._write_schema_to_s3(schema)

        trigger_bedrock_ingestion(
            bucket=settings.s3_bucket,
            prefix="schema/"
        )

        logger.info("Schema ingestion completed successfully")
        return {
            "run_id": run_id,
            "schema_s3_uri": s3_uri,
            "status": "SUCCESS"
        }

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate(self):
        required = [
            settings.databricks_host,
            settings.databricks_token,
            settings.databricks_cluster_id,
            settings.databricks_schema_extractor_script,
            settings.s3_bucket
        ]

        if not all(required):
            raise RuntimeError("Missing required Databricks or S3 configuration")

    # ------------------------------------------------------------------
    # Databricks Job Submission
    # ------------------------------------------------------------------
    def _submit_job(self) -> int:
        payload = {
            "run_name": "INAAS - Schema Extraction",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_schema_extractor_script
            }
        }

        logger.info("Submitting Databricks schema extraction job")

        resp = requests.post(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            json=payload,
            timeout=30
        )
        resp.raise_for_status()

        run_id = resp.json()["run_id"]
        logger.info("Databricks run_id: %s", run_id)
        return run_id

    # ------------------------------------------------------------------
    # Poll + Fetch Logs
    # ------------------------------------------------------------------
    def _wait_and_fetch_logs(self, run_id: int) -> str:
        while True:
            status = requests.get(
                f"https://{settings.databricks_host}/api/2.1/jobs/runs/get",
                headers={"Authorization": f"Bearer {settings.databricks_token}"},
                params={"run_id": run_id},
                timeout=30
            )
            status.raise_for_status()

            state = status.json()["state"]
            life_cycle = state["life_cycle_state"]
            result = state.get("result_state")

            logger.info(
                "Run %s | life_cycle=%s | result=%s",
                run_id, life_cycle, result
            )

            if life_cycle == "TERMINATED":
                if result != "SUCCESS":
                    raise RuntimeError("Databricks job failed")
                break

            time.sleep(5)

        output = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id},
            timeout=30
        )
        output.raise_for_status()

        stdout = output.json().get("logs", "")
        logger.info("========== DATABRICKS STDOUT ==========")
        print(stdout)

        return stdout

    # ------------------------------------------------------------------
    # Schema Extraction from Logs
    # ------------------------------------------------------------------
    def _extract_schema(self, stdout: str) -> dict:
        start = "INAAS_SCHEMA_EXTRACTION_START"
        end = "INAAS_SCHEMA_EXTRACTION_END"

        if start not in stdout or end not in stdout:
            raise RuntimeError("Schema markers not found in Databricks output")

        payload = stdout.split(start)[1].split(end)[0].strip()
        lines = payload.splitlines()

        json_line = next(
            (line for line in lines if line.strip().startswith("{")),
            None
        )

        if not json_line:
            raise RuntimeError("Schema JSON not found in Databricks logs")

        schema = json.loads(json_line)

        return {
            "file_id": schema["file_path"],
            "file_path": schema["file_path"],
            "format": schema["format"],
            "columns": schema["columns"],
            "sample_rows": schema.get("sample_rows", []),
            "ingested_at": datetime.now(timezone.utc).isoformat()
        }

    # ------------------------------------------------------------------
    # Persist Schema to S3
    # ------------------------------------------------------------------
    def _write_schema_to_s3(self, schema: dict) -> str:
        s3 = boto3.client("s3")

        base = schema["file_path"].split("/")[-1].split(".")[0]
        key = f"schema/{base}.json"

        s3.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=json.dumps(schema, indent=2),
            ContentType="application/json"
        )

        s3_uri = f"s3://{settings.s3_bucket}/{key}"
        logger.info("Schema written to %s", s3_uri)

        return s3_uri
