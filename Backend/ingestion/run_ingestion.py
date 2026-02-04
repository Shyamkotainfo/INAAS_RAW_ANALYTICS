from ingestion.databricks_ingestion_executor import DatabricksIngestionExecutor
from ingestion.bedrock_ingestor import BedrockIngestor
from logger.logger import get_logger

logger = get_logger(__name__)

def main():
    logger.info("Starting INAAS schema ingestion via Databricks")

    executor = DatabricksIngestionExecutor()
    run_info = executor.run()

    logger.info(f"Databricks job submitted: {run_info}")

    BedrockIngestor().ingest()

    logger.info("Schema ingestion pipeline completed")

if __name__ == "__main__":
    main()
