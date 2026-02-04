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
        logger.info(f"Bedrock ingestion job started: {job_id}")

        while True:
            job = self.client.get_ingestion_job(
                knowledgeBaseId=settings.bedrock_kb_id,
                dataSourceId=settings.bedrock_data_source_id,
                ingestionJobId=job_id
            )

            status = job["ingestionJob"]["status"]
            logger.info(f"Bedrock ingestion status: {status}")

            if status == "COMPLETE":
                logger.info("Bedrock ingestion completed")
                return job_id

            time.sleep(10)
