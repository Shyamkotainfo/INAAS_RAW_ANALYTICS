# Backend/execution/emr_serverless_executor.py

import json
import time
import uuid
import boto3
from typing import List

from config.settings import settings
from execution.base_executor import BaseExecutor
from logger.logger import get_logger

logger = get_logger(__name__)


class EMRServerlessExecutor(BaseExecutor):
    """
    Executes Spark jobs on EMR Serverless.
    Backend NEVER runs Spark itself.
    """

    def __init__(self):
        logger.info("Initializing EMRServerlessExecutor")

        self.client = boto3.client(
            "emr-serverless",
            region_name=settings.aws_region
        )

        self.s3 = boto3.client(
            "s3",
            region_name=settings.aws_region
        )

        self.application_id = settings.emr_application_id
        self.execution_role_arn = settings.emr_execution_role_arn
        self.log_s3_uri = settings.emr_log_s3_uri

        logger.info(
            "EMR Serverless configured | "
            f"application_id={self.application_id}, "
            f"log_s3_uri={self.log_s3_uri}"
        )

    # --------------------------------------------------
    # Interface contract
    # --------------------------------------------------

    def load_df(self, file_path: str):
        """
        EMR Serverless executor does NOT support local DataFrame access.
        """
        logger.error("load_df() called on EMRServerlessExecutor")
        raise RuntimeError(
            "load_df() is not supported in EMRServerlessExecutor. "
            "DataFrames are only available inside EMR Serverless jobs."
        )

    # --------------------------------------------------
    # Internal helpers
    # --------------------------------------------------

    def _submit_job(self, entry_point: str, args: list[str]) -> str:
        logger.info("Submitting EMR Serverless job")
        logger.debug(f"Entry point: {entry_point}")
        logger.debug(f"Arguments: {args}")

        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": entry_point,
                    "entryPointArguments": args,
                    "sparkSubmitParameters": (
                        "--conf spark.executor.instances=1 "
                        "--conf spark.executor.cores=2 "
                        "--conf spark.executor.memory=2g "
                        "--conf spark.driver.cores=1 "
                        "--conf spark.driver.memory=2g "
                    )
                }
            },

        configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": self.log_s3_uri
                    }
                }
            }
        )

        job_run_id = response["jobRunId"]
        logger.info(f"EMR job submitted | job_run_id={job_run_id}")

        return job_run_id

    def _wait_for_completion(self, job_run_id: str):
        logger.info(f"Waiting for EMR job completion | job_run_id={job_run_id}")

        while True:
            resp = self.client.get_job_run(
                applicationId=self.application_id,
                jobRunId=job_run_id
            )

            state = resp["jobRun"]["state"]
            logger.debug(f"EMR job state | job_run_id={job_run_id} | state={state}")

            if state == "SUCCESS":
                logger.info(f"EMR job succeeded | job_run_id={job_run_id}")
                return

            if state in ("FAILED", "CANCELLED"):
                details = resp["jobRun"].get("stateDetails")
                logger.error(
                    f"EMR job failed | job_run_id={job_run_id} | state={state} | details={details}"
                )
                raise RuntimeError(f"EMR job failed: {state}\n{details}")

            time.sleep(5)

    def _upload_generated_code(self, pyspark_code: str) -> str:
        job_id = str(uuid.uuid4())
        key = f"emr/generated/{job_id}.py"

        logger.info("Uploading generated PySpark code to S3")
        logger.debug(f"S3 key: s3://{settings.s3_bucket}/{key}")

        self.s3.put_object(
            Bucket=settings.s3_bucket,
            Key=key,
            Body=pyspark_code.encode("utf-8")
        )

        return f"s3://{settings.s3_bucket}/{key}"

    def _read_result(self, job_id: str):
        prefix = f"emr/results/{job_id}/"

        resp = self.s3.list_objects_v2(
            Bucket=settings.s3_bucket,
            Prefix=prefix
        )

        if "Contents" not in resp:
            raise RuntimeError(f"No results found in S3 for job_id={job_id}")

        # Read all part-* files
        results = []

        for obj in resp["Contents"]:
            key = obj["Key"]

            if "part-" not in key:
                continue

            body = self.s3.get_object(
                Bucket=settings.s3_bucket,
                Key=key
            )["Body"].read().decode("utf-8")

            for line in body.splitlines():
                if line.strip():
                    results.append(json.loads(line))

        return results

    # --------------------------------------------------
    # Interface implementation
    # --------------------------------------------------

    def execute_query(self, file_path: str, pyspark_code: str) -> List[dict]:
        # job_id = str(uuid.uuid4())
        job_id = "e09eaacc-c32e-4f70-9e09-018c01c2eaea"
        logger.info(f"Executing QUERY on EMR | job_id={job_id}")
        logger.debug(f"File path: {file_path}")

        # generated_code_s3 = self._upload_generated_code(pyspark_code)
        #
        # job_run_id = self._submit_job(
        #     entry_point=settings.emr_job_entry_s3,
        #     args=[
        #         "query",
        #         file_path,
        #         generated_code_s3,
        #         job_id
        #     ]
        # )
        #
        # self._wait_for_completion(job_run_id)
        return self._read_result(job_id)

    def execute_profile(self, file_path: str) -> dict:
        job_id = str(uuid.uuid4())
        logger.info(f"Executing PROFILE on EMR | job_id={job_id}")
        logger.debug(f"File path: {file_path}")

        job_run_id = self._submit_job(
            settings.emr_job_entry_s3,
            ["profile", file_path, job_id]
        )

        self._wait_for_completion(job_run_id)
        return self._read_result(job_id)

    def execute_quality(self, file_path: str) -> dict:
        job_id = str(uuid.uuid4())
        logger.info(f"Executing QUALITY on EMR | job_id={job_id}")
        logger.debug(f"File path: {file_path}")

        job_run_id = self._submit_job(
            settings.emr_job_entry_s3,
            ["quality", file_path, job_id]
        )

        self._wait_for_completion(job_run_id)
        return self._read_result(job_id)
