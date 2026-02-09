# Backend/rag/bedrock_ingestor.py

import time
import boto3
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
        logger.info("Starting Bedrock ingestion")

        response = self.client.start_ingestion_job(
            knowledgeBaseId=settings.bedrock_kb_id,
            dataSourceId=settings.bedrock_data_source_id
        )

        job_id = response["ingestionJob"]["ingestionJobId"]
        logger.info("Bedrock ingestion job started: %s", job_id)

        while True:
            job = self.client.get_ingestion_job(
                knowledgeBaseId=settings.bedrock_kb_id,
                dataSourceId=settings.bedrock_data_source_id,
                ingestionJobId=job_id
            )

            status = job["ingestionJob"]["status"]
            logger.info("Bedrock ingestion status: %s", status)

            if status == "COMPLETE":
                logger.info("Bedrock ingestion completed")
                return job_id

            if status in {"FAILED", "STOPPED"}:
                raise RuntimeError(f"Bedrock ingestion failed: {status}")

            time.sleep(10)


# -------------------------------------------------
# Public helper (THIS is what executors import)
# -------------------------------------------------
def trigger_bedrock_ingestion(bucket: str, prefix: str):
    """
    Bucket + prefix are already configured in the Bedrock data source.
    This function simply triggers ingestion.
    """
    logger.info(
        "Triggering Bedrock ingestion for s3://%s/%s",
        bucket, prefix
    )

    ingestor = BedrockIngestor()
    return ingestor.ingest()
