# Backend/rag/bedrock_ingestor.py

import boto3
from botocore.exceptions import ClientError
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class BedrockIngestor:
    def __init__(self):
        self.client = boto3.client(
            "bedrock-agent",
            region_name=settings.aws_region
        )

    def ingest(self):
        """
        Triggers ingestion job in Bedrock Knowledge Base.
        This method is NON-BLOCKING.
        """

        logger.info("Starting Bedrock ingestion")

        try:
            response = self.client.start_ingestion_job(
                knowledgeBaseId=settings.bedrock_kb_id,
                dataSourceId=settings.bedrock_data_source_id
            )

            job_id = response["ingestionJob"]["ingestionJobId"]
            logger.info("Bedrock ingestion job started: %s", job_id)

            return {
                "status": "STARTED",
                "job_id": job_id
            }

        except ClientError as e:
            error_code = e.response["Error"]["Code"]

            if error_code == "ConflictException":
                logger.info(
                    "Ingestion already running for this data source. Skipping new trigger."
                )
                return {
                    "status": "ALREADY_RUNNING"
                }

            logger.error("Bedrock ingestion failed: %s", str(e))
            raise


# -------------------------------------------------
# Public helper
# -------------------------------------------------
def trigger_bedrock_ingestion(bucket: str, prefix: str):
    """
    Bucket + prefix are already configured in Bedrock data source.
    This just triggers ingestion.
    """

    logger.info(
        "Triggering Bedrock ingestion for s3://%s/%s",
        bucket,
        prefix
    )

    ingestor = BedrockIngestor()
    return ingestor.ingest()