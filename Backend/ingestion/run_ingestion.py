import logging
from config.settings import settings
from logger.logger import get_logger

from ingestion.databricks_ingestion_executor import DatabricksIngestionExecutor
from ingestion.emr_ingestion_executor import EMRIngestionExecutor

logger = get_logger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("Starting INAAS Schema Ingestion")
    logger.info(f"Execution mode: {settings.execution_mode}")
    logger.info("=" * 60)

    if settings.execution_mode == "databricks":
        logger.info("Using Databricks for schema ingestion")

        if not settings.databricks_cluster_id:
            raise RuntimeError("DATABRICKS_CLUSTER_ID is not set")

        executor = DatabricksIngestionExecutor()
        result = executor.run()

        logger.info("Databricks ingestion job submitted")
        logger.info(result)

    elif settings.execution_mode == "emr":
        logger.info("Using EMR Serverless for schema ingestion")

        if not settings.emr_application_id:
            raise RuntimeError("EMR_APPLICATION_ID is not set")

        if not settings.emr_schema_extractor_script:
            raise RuntimeError("EMR_SCHEMA_EXTRACTOR_SCRIPT is not set")

        executor = EMRIngestionExecutor()
        result = executor.run()

        logger.info("EMR ingestion job submitted")
        logger.info(result)

    else:
        raise RuntimeError(
            f"Unsupported execution mode: {settings.execution_mode}"
        )


if __name__ == "__main__":
    main()
