import json
from pathlib import Path
import boto3
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class MetadataUploader:
    def __init__(self):
        self.bucket = settings.s3_bucket
        self.s3 = boto3.client("s3", region_name=settings.aws_region)

    def upload(self, metadata: dict, key: str) -> str:
        # key example: employee_details.csv
        base_name = Path(key).stem  # employee_details
        metadata_key = f"schema/{base_name}.json"

        logger.info(f"Uploading metadata to s3://{self.bucket}/{metadata_key}")

        self.s3.put_object(
            Bucket=self.bucket,
            Key=metadata_key,
            Body=json.dumps(metadata, indent=2),
            ContentType="application/json"
        )
        return metadata_key
