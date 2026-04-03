# Backend/core/query_orchestrator.py

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from query_generation.pyspark_generator import PySparkCodeGenerator, IrrelevantQueryError
from summarization.result_summarizer import ResultSummarizer

logger = get_logger(__name__)

MAX_RETRIES = 3


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
        # Trigger Bedrock ingestion (SAFE — non-blocking)
        # ----------------------------
        try:
            ingestion_status = trigger_bedrock_ingestion(
                bucket=settings.s3_bucket,
                prefix="schema/"
            )
            logger.info("Bedrock ingestion trigger response: %s", ingestion_status)
        except Exception as e:
            logger.warning(
                "Bedrock ingestion failed but profiling succeeded: %s", str(e)
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
    # QUERY EXECUTION  (with retry + correction loop)
    # =====================================================
    def run(self, user_input: str, dataset_id: str) -> dict:

        if not self.active_file or self.active_file["file_id"] != dataset_id:
            raise RuntimeError("Dataset not loaded. Please run profiling first.")

        context = {
            "file_path": self.active_file["file_path"],
            "format": self.active_file["format"],
            "columns": self.active_file["columns"]
        }

        # --------------------------------------------------
        # Step 1 — Relevance guard + initial code generation
        # --------------------------------------------------
        try:
            pyspark_code = self.codegen.generate(user_input, context)
        except IrrelevantQueryError as e:
            logger.info("Irrelevant query blocked: %s", str(e))
            return {
                "user_input": user_input,
                "irrelevant": True,
                "message": (
                    "This question does not appear to relate to the loaded dataset. "
                    "Please ask something about the data — its structure, quality, "
                    "trends, distributions, or business metrics."
                ),
                "pyspark": None,
                "results": None,
                "insights": None,
            }

        # --------------------------------------------------
        # Step 2 — Execute with up to MAX_RETRIES correction attempts
        # --------------------------------------------------
        last_error = None
        last_code = pyspark_code

        for attempt in range(1, MAX_RETRIES + 1):
            logger.info("Execution attempt %d / %d", attempt, MAX_RETRIES)

            execution = self.executor.execute_query(context, last_code)

            if execution["status"] == "SUCCESS":
                result = execution["result"]

                summary = self.summarizer.summarize(
                    question=user_input,
                    result=result,
                    mode="query"
                )

                return {
                    "user_input": user_input,
                    "pyspark": last_code,
                    "results": result,
                    "insights": summary,
                    "attempts": attempt,
                }

            # FAILED — capture error and try to correct
            last_error = execution.get("error", "Unknown execution error")

            logger.warning(
                "Execution attempt %d failed: %s", attempt, last_error
            )

            if attempt < MAX_RETRIES:
                logger.info("Requesting LLM correction for attempt %d", attempt + 1)
                try:
                    corrected_code = self.codegen.correct(
                        question=user_input,
                        context=context,
                        failing_code=last_code,
                        error_message=last_error
                    )
                    last_code = corrected_code
                except RuntimeError as e:
                    logger.warning(
                        "LLM correction produced invalid code on attempt %d: %s",
                        attempt, str(e)
                    )
                    # Keep last_code unchanged — try again with same code is pointless,
                    # but we break to avoid infinite correction loops on structural failures
                    break

        # --------------------------------------------------
        # Step 3 — All retries exhausted
        # --------------------------------------------------
        logger.error(
            "All %d execution attempts exhausted. Last error: %s", MAX_RETRIES, last_error
        )

        return {
            "user_input": user_input,
            "error": last_error,
            "query": last_code,
            "pyspark": last_code,
            "results": None,
            "insights": None,
            "attempts": MAX_RETRIES,
        }