# Backend/execution/emr_executor.py

import time
import json
import boto3

from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class EMRExecutor:
    def __init__(self):
        self.client = boto3.client(
            "emr-serverless",
            region_name=settings.aws_region
        )

    def execute(self, context: dict, pyspark_code: str):
        """
        Submits PySpark code to EMR Serverless and waits for completion.
        Results are printed in EMR driver logs.
        """

        payload = {
            "file_path": context["file_path"],
            "format": context["format"],
            "pyspark_code": pyspark_code
        }

        logger.info("Submitting EMR Serverless job")
        logger.info(payload)

        response = self.client.start_job_run(
            applicationId=settings.emr_application_id,
            executionRoleArn=settings.emr_execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": settings.emr_run_query_script,
                    "entryPointArguments": [
                        json.dumps(payload)
                    ]
                }
            }
        )

        job_run_id = response["jobRunId"]
        logger.info(f"EMR job submitted: {job_run_id}")

        # -----------------------------
        # Poll job status
        # -----------------------------
        while True:
            job = self.client.get_job_run(
                applicationId=settings.emr_application_id,
                jobRunId=job_run_id
            )

            state = job["jobRun"]["state"]
            logger.info(f"EMR job state: {state}")

            if state in ("SUCCESS", "FAILED", "CANCELLED"):
                break

            time.sleep(5)

        if state != "SUCCESS":
            error = job["jobRun"].get("stateDetails", "")
            logger.error(f"EMR job failed: {error}")
            return {
                "status": state,
                "error": error,
                "jobRunId": job_run_id
            }

        logger.info("EMR job completed successfully")

        return {
            "status": "SUCCESS",
            "jobRunId": job_run_id,
            "note": "Results printed in EMR driver logs"
        }
