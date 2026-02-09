# Backend/config/settings.py

"""
Application settings for INAAS
Supports:
- Databricks Spark (interactive)
- EMR Serverless Spark (batch)
"""

import os
from typing import Optional
from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    # =========================
    # Environment
    # =========================
    environment: str = Field("development", env="ENVIRONMENT")
    debug: bool = Field(False, env="DEBUG")

    # =========================
    # Execution Mode
    # =========================
    execution_mode: str = Field("databricks", env="EXECUTION_MODE")

    # =========================
    # AWS
    # =========================
    aws_region: str = Field("us-east-1", env="AWS_REGION")

    aws_access_key_id: Optional[str] = Field(None, env="AWS_ACCESS_KEY_ID")
    aws_secret_access_key: Optional[str] = Field(None, env="AWS_SECRET_ACCESS_KEY")
    aws_session_token: Optional[str] = Field(None, env="AWS_SESSION_TOKEN")

    # =========================
    # S3
    # =========================
    s3_raw_bucket: str = Field(..., env="S3_RAW_BUCKET")
    s3_bucket: str = Field(..., env="S3_BUCKET")

    # =========================
    # Bedrock
    # =========================
    bedrock_kb_id: str = Field(..., env="BEDROCK_KB_ID")
    bedrock_data_source_id: str = Field(..., env="BEDROCK_DATA_SOURCE_ID")
    bedrock_model: str = Field("amazon.nova-pro-v1:0", env="BEDROCK_MODEL")

    # =========================
    # Databricks Spark
    # =========================
    databricks_host: Optional[str] = Field(None, env="DATABRICKS_HOST")
    databricks_token: Optional[str] = Field(None, env="DATABRICKS_TOKEN")
    databricks_cluster_id: Optional[str] = Field(None, env="DATABRICKS_CLUSTER_ID")

    databricks_run_query_script: str = Field(
        "dbfs:/inaas/jobs/run_query.py",
        env="DATABRICKS_RUN_QUERY_SCRIPT"
    )

    databricks_schema_extractor_script: str = Field(
        "dbfs:/inaas/jobs/schema_extractor.py",
        env="DATABRICKS_SCHEMA_EXTRACTOR_SCRIPT"
    )

    # =========================
    # EMR Serverless
    # =========================
    emr_application_id: Optional[str] = Field(None, env="EMR_APPLICATION_ID")
    emr_execution_role_arn: Optional[str] = Field(None, env="EMR_EXECUTION_ROLE_ARN")
    emr_schema_extractor_script: Optional[str] = Field(None, env="EMR_SCHEMA_EXTRACTOR_SCRIPT")
    emr_run_query_script: Optional[str] = Field(None, env="EMR_RUN_QUERY_SCRIPT")

    # =========================
    # Validators
    # =========================
    @validator("execution_mode")
    def validate_execution_mode(cls, v):
        allowed = {"databricks", "emr"}
        if v not in allowed:
            raise ValueError(f"EXECUTION_MODE must be one of {allowed}")
        return v

    def configure_aws_credentials(self):
        if self.aws_access_key_id and self.aws_secret_access_key:
            os.environ["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
            if self.aws_session_token:
                os.environ["AWS_SESSION_TOKEN"] = self.aws_session_token
        os.environ["AWS_DEFAULT_REGION"] = self.aws_region

    class Config:
        env_file = ".env"
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
