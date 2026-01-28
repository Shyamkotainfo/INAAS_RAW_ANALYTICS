import boto3
from typing import List
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class S3Reader:
    def __init__(self):
        self.bucket = settings.s3_raw_bucket
        self.s3 = boto3.client("s3", region_name=settings.aws_region)

    def list_files(self, prefix: str = "") -> List[str]:
        logger.info(f"Listing files in s3://{self.bucket}/{prefix}")

        paginator = self.s3.get_paginator("list_objects_v2")
        files = []

        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith("/"):
                    files.append(obj["Key"])

        logger.info(f"Found {len(files)} files")
        return files

    def get_last_modified(self, key: str) -> str:
        obj = self.s3.head_object(Bucket=self.bucket, Key=key)
        return obj["LastModified"].isoformat()
