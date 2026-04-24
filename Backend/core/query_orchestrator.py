# Backend/core/query_orchestrator.py

import hashlib

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from prompt.pyspark_code_gen import build_semantic_context
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
    def attach_file(self, file_id: str, file_path: str, file_format: str, context: str = None):

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

        semantic_context = None
        semantic_context_hash = None
        if context:
            logger.info("Wiki root configured for targeted retrieval: %s", context)
            schema["semantic_context_path"] = context

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
            "columns": schema["columns"],
            "semantic_context": semantic_context,
            "semantic_context_path": context,
            "semantic_context_hash": semantic_context_hash
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
            "columns": self.active_file["columns"],
            "semantic_context": self.active_file.get("semantic_context"),
            "semantic_context_path": self.active_file.get("semantic_context_path"),
        }

        if context.get("semantic_context_path"):
            columns = ", ".join(c["name"] for c in context["columns"])
            context["semantic_context"] = build_semantic_context(
                question=user_input,
                columns=columns,
                top_k=3
            )
            self.active_file["semantic_context"] = context["semantic_context"]
            self.active_file["semantic_context_hash"] = self._hash_text(context["semantic_context"])

        context_debug = self._build_context_debug(context)
        logger.info(
            "Passing semantic context to LLM | used=%s | path=%s | chars=%d | hash=%s",
            context_debug["context_used"],
            context_debug["context_path"],
            context_debug["context_chars"],
            context_debug["context_hash"]
        )

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
                "text_results": None,
                "context_debug": context_debug,
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

                # Run both LLM insights and explanation in parallel to minimize latency
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=2) as tpool:
                    future_summary = tpool.submit(self.summarizer.summarize, user_input, result, "query")
                    future_explain = tpool.submit(self.summarizer.explain, user_input, result)
                    
                    summary = future_summary.result()
                    text_results = future_explain.result()

                return {
                    "user_input": user_input,
                    "pyspark": last_code,
                    "results": result,
                    "insights": summary,
                    "text_results": text_results,
                    "attempts": attempt,
                    "context_debug": context_debug,
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
            "text_results": None,
            "attempts": MAX_RETRIES,
            "context_debug": context_debug,
        }

    def _build_context_debug(self, context: dict) -> dict:
        semantic_context = context.get("semantic_context") or ""
        return {
            "context_used": bool(semantic_context),
            "context_path": self.active_file.get("semantic_context_path") if self.active_file else None,
            "context_chars": len(semantic_context),
            "context_hash": self.active_file.get("semantic_context_hash") if self.active_file else None,
            "context_marker_present": "CONTEXT_MARKER" in semantic_context,
        }

    def _hash_text(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
