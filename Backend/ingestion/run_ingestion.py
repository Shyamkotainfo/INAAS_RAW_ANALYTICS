import logging
from config.settings import settings
from logger.logger import get_logger

from ingestion.databricks_ingestion_executor import DatabricksIngestionExecutor
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

    else:
        raise RuntimeError(
            f"Unsupported execution mode: {settings.execution_mode}"
        )


if __name__ == "__main__":
    main()
