from pyspark.sql import SparkSession
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


def get_spark() -> SparkSession:
    logger.info("Starting LOCAL SparkSession with S3 support")

    settings.configure_aws_credentials()

    spark = (
        SparkSession.builder
        .appName("LocalSchemaExtractor")
        # --- S3A CONFIG ---
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com")
        # --- Performance ---
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    logger.info("SparkSession started successfully")
    return spark
