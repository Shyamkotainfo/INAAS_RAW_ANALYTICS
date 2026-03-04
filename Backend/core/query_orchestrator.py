# Backend/core/query_orchestrator.py

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from query_generation.pyspark_generator import PySparkCodeGenerator
from summarization.result_summarizer import ResultSummarizer

logger = get_logger(__name__)


class QueryOrchestrator:

    def __init__(self):
        self.executor = DatabricksExecutor()
        self.codegen = PySparkCodeGenerator()
        self.summarizer = ResultSummarizer()
        self.active_file = None

    # =====================================================
    # FILE INGESTION
    # =====================================================
    def attach_file(self, file_id: str, file_path: str, file_format: str):

        logger.info("Starting ingestion for file: %s", file_path)

        # ----------------------------
        # Databricks profiling
        # ----------------------------
        ingestion = self.executor.ingest_and_profile(
            file_id=file_id,
            file_path=file_path,
            file_format=file_format
        )

        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema = ingestion["schema"]
        profiling = ingestion["profiling"]

        # ----------------------------
        # Upload schema to S3
        # ----------------------------
        uploader = MetadataUploader()
        uploader.upload(schema, file_id)

        # ----------------------------
        # Trigger Bedrock ingestion (SAFE)
        # ----------------------------
        try:
            ingestion_status = trigger_bedrock_ingestion(
                bucket=settings.s3_bucket,
                prefix="schema/"
            )

            logger.info(
                "Bedrock ingestion trigger response: %s",
                ingestion_status
            )

        except Exception as e:
            # IMPORTANT: Do not fail profiling if Bedrock fails
            logger.warning(
                "Bedrock ingestion failed but profiling succeeded: %s",
                str(e)
            )

        logger.info("Schema uploaded to S3 and ingestion process handled")

        # ----------------------------
        # Store active file in memory
        # ----------------------------
        self.active_file = {
            "file_id": file_id,
            "file_path": file_path,
            "format": file_format,
            "columns": schema["columns"]
        }

        return profiling

    # =====================================================
    # QUERY EXECUTION
    # =====================================================
    def run(self, user_input: str, dataset_id: str) -> dict:

        if not self.active_file or self.active_file["file_id"] != dataset_id:
            raise RuntimeError("Dataset not loaded. Please run profiling first.")

        context = {
            "file_path": self.active_file["file_path"],
            "format": self.active_file["format"],
            "columns": self.active_file["columns"]
        }

        pyspark_code = self.codegen.generate(user_input, context)

        execution = self.executor.execute_query(context, pyspark_code)

        if execution["status"] != "SUCCESS":
            return {
                "user_input": user_input,
                "pyspark": pyspark_code,
                "results": None,
                "insights": None,
                "error": execution.get("error"),
            }

        result = execution["result"]

        summary = self.summarizer.summarize(
            question=user_input,
            result=result,
            mode="query"
        )

        return {
            "user_input": user_input,
            "pyspark": pyspark_code,
            "results": result,
            "insights": summary,
        }