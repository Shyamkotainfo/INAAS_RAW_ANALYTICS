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
        # Trigger Bedrock ingestion
        # ----------------------------
        trigger_bedrock_ingestion(
            bucket=settings.s3_bucket,
            prefix="schema/"
        )

        logger.info("Schema uploaded to S3 and Bedrock ingestion triggered")

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
    def run(self, question: str) -> dict:

        if not self.active_file:
            raise RuntimeError("No file uploaded. Please upload a file first.")

        context = {
            "file_path": self.active_file["file_path"],
            "format": self.active_file["format"],
            "columns": self.active_file["columns"]
        }

        pyspark_code = self.codegen.generate(question, context)

        execution = self.executor.execute_query(context, pyspark_code)

        if execution["status"] != "SUCCESS":
            return {
                "user_input": question,
                "pyspark": pyspark_code,
                "results": None,
                "insights": None,
                "error": execution.get("error"),
            }

        result = execution["result"]

        summary = self.summarizer.summarize(
            question=question,
            result=result,
            mode="query"
        )

        return {
            "user_input": question,
            "pyspark": pyspark_code,
            "results": result,
            "insights": summary,
        }
