import boto3
from config.settings import settings


class EMRIngestionExecutor:
    def run(self):
        client = boto3.client(
            "emr-serverless",
            region_name=settings.aws_region
        )

        response = client.start_job_run(
            applicationId=settings.emr_application_id,
            executionRoleArn=settings.emr_execution_role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": settings.emr_schema_extractor_script
                }
            }
        )

        return response
