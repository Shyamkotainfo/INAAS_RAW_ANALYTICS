# Backend/layers/common.py

import json
import boto3
from botocore.exceptions import ClientError
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)

# Format bucket constants
STRUCTURED       = "structured"        # CSV, Parquet, Delta, XLSX
SEMI_STRUCTURED  = "semi_structured"   # JSON, XML, Avro
UNSTRUCTURED     = "unstructured"      # PDF, DOCX, TXT, images

_FORMAT_MAP = {
    "csv":     STRUCTURED,
    "parquet": STRUCTURED,
    "delta":   STRUCTURED,
    "xlsx":    STRUCTURED,
    "xls":     STRUCTURED,
    "json":    SEMI_STRUCTURED,
    "xml":     SEMI_STRUCTURED,
    "avro":    SEMI_STRUCTURED,
    "pdf":     UNSTRUCTURED,
    "docx":    UNSTRUCTURED,
    "doc":     UNSTRUCTURED,
    "txt":     UNSTRUCTURED,
}

def _detect_format_bucket(file_format: str) -> str:
    """Map a file extension to a format bucket (structured/semi/unstructured)."""
    return _FORMAT_MAP.get(file_format.lower(), STRUCTURED)

def load_metadata_from_s3(file_id: str) -> dict | None:
    """Load schema metadata from S3."""
    s3 = boto3.client("s3", region_name=settings.aws_region)
    key = f"schema/{file_id}.json"
    try:
        resp = s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return json.loads(resp["Body"].read())
    except ClientError:
        logger.warning("Metadata not found in S3 for %s", file_id)
        return None

def load_profile_from_s3(file_id: str) -> dict | None:
    """Load pre-computed profile stats from S3."""
    s3 = boto3.client("s3", region_name=settings.aws_region)
    key = f"profile/{file_id}/profile.json"
    try:
        resp = s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return json.loads(resp["Body"].read())
    except ClientError:
        logger.warning("Profile not found in S3 for %s", file_id)
        return None
