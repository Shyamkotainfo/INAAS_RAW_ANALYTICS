"""
Application settings and configuration management
LOCAL PySpark + AWS + Bedrock
"""

import os
from typing import Optional, List
from functools import lru_cache

from pydantic_settings import BaseSettings
from pydantic import Field, validator


class Settings(BaseSettings):
    """Application settings with validation"""

    # -------------------- Environment --------------------
    environment: str = Field(default="development", env="ENVIRONMENT")
    debug: bool = Field(default=False, env="DEBUG")

    # -------------------- Server (optional) --------------------
    host: str = Field(default="0.0.0.0", env="HOST")
    port: int = Field(default=8000, env="PORT")

    # -------------------- CORS --------------------
    allowed_origins: List[str] = Field(
        default=["http://localhost:3000"],
        env="ALLOWED_ORIGINS"
    )

    # -------------------- Logging --------------------
    log_level: str = Field(default="INFO", env="LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="LOG_FILE")

    # -------------------- AWS Core --------------------
    aws_region: str = Field(default="us-east-1", env="AWS_REGION")

    aws_access_key_id: Optional[str] = Field(
        default=None, env="AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None, env="AWS_SECRET_ACCESS_KEY"
    )
    aws_session_token: Optional[str] = Field(
        default=None, env="AWS_SESSION_TOKEN"
    )

    # -------------------- S3 --------------------
    s3_bucket: str = Field(..., env="S3_BUCKET")
    s3_raw_bucket: str = Field(..., env="S3_RAW_BUCKET")

    # -------------------- Bedrock --------------------
    bedrock_kb_id: str = Field(..., env="BEDROCK_KB_ID")
    bedrock_data_source_id: str = Field(..., env="BEDROCK_DATA_SOURCE_ID")
    bedrock_model: str = Field(
        default="amazon.nova-pro-v1:0",
        env="BEDROCK_MODEL"
    )
    bedrock_embedding_model: str = Field(
        default="amazon.titan-embed-text-v2:0",
        env="BEDROCK_EMBEDDING_MODEL"
    )

    # -------------------- Execution --------------------
    execution_mode: str = Field(
        default="local",
        env="EXECUTION_MODE"
    )

    emr_application_id: str = Field(..., env="EMR_APPLICATION_ID")
    emr_execution_role_arn: str = Field(..., env="EMR_EXECUTION_ROLE_ARN")
    emr_job_entry_s3: str = Field(..., env="EMR_JOB_ENTRY_S3")
    emr_log_s3_uri: str = Field(..., env="EMR_LOG_S3_URI")

    # -------------------- Validators --------------------
    @validator("execution_mode")
    def validate_execution_mode(cls, v):
        allowed = {"local", "databricks", "emr"}
        if v not in allowed:
            raise ValueError(f"EXECUTION_MODE must be one of {allowed}")
        return v

    @validator("allowed_origins", pre=True)
    def parse_allowed_origins(cls, v):
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",")]
        return v

    @validator("environment")
    def validate_environment(cls, v):
        allowed = {"development", "staging", "production"}
        if v not in allowed:
            raise ValueError(f"Environment must be one of {allowed}")
        return v

    @validator("log_level")
    def validate_log_level(cls, v):
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        v = v.upper()
        if v not in allowed:
            raise ValueError(f"Log level must be one of {allowed}")
        return v

    # -------------------- AWS Credential Injection --------------------
    def configure_aws_credentials(self):
        """
        Inject AWS credentials into environment for PySpark (s3a)
        Only needed if credentials are provided via env vars.
        """
        if self.aws_access_key_id and self.aws_secret_access_key:
            os.environ["AWS_ACCESS_KEY_ID"] = self.aws_access_key_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws_secret_access_key
            if self.aws_session_token:
                os.environ["AWS_SESSION_TOKEN"] = self.aws_session_token

        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

    class Config:
        env_file = ".env"
        case_sensitive = False
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


# Backward compatibility
settings = get_settings()
