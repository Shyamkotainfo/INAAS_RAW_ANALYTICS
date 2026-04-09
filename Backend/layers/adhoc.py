# Backend/layers/adhoc.py

from concurrent.futures import ThreadPoolExecutor
from logger.logger import get_logger
from query_generation.pyspark_generator import IrrelevantQueryError

logger = get_logger(__name__)

MAX_RETRIES = 3

class AdhocLayer:
    def __init__(self, executor, codegen, summarizer):
        self.executor = executor
        self.codegen = codegen
        self.summarizer = summarizer

    def handle(self, user_input: str, dataset_id: str, format_bucket: str, active_file: dict) -> dict:
        """Generate PySpark on demand and execute on Databricks."""
        context = {
            "file_path": active_file["file_path"],
            "format":    active_file["format"],
            "columns":   active_file["columns"],
        }

        try:
            pyspark_code = self.codegen.generate(user_input, context)
        except IrrelevantQueryError as e:
            logger.info("Irrelevant query blocked: %s", e)
            return {
                "route":       "IRRELEVANT",
                "user_input":  user_input,
                "irrelevant":  True,
                "message": (
                    "This question does not appear to relate to the loaded dataset. "
                    "Please ask something about the data — its structure, quality, "
                    "trends, distributions, or business metrics."
                ),
                "pyspark": None, "results": None, "insights": None, "text_results": None,
            }

        last_error = None
        last_code  = pyspark_code

        for attempt in range(1, MAX_RETRIES + 1):
            logger.info("Execution attempt %d / %d", attempt, MAX_RETRIES)
            execution = self.executor.execute_query(context, last_code)

            if execution["status"] == "SUCCESS":
                result = execution["result"]
                with ThreadPoolExecutor(max_workers=2) as tpool:
                    future_summary = tpool.submit(self.summarizer.summarize, user_input, result, "query")
                    future_explain = tpool.submit(self.summarizer.explain, user_input, result)
                    summary      = future_summary.result()
                    text_results = future_explain.result()

                return {
                    "route":        "ADHOC",
                    "format_bucket": format_bucket,
                    "user_input":   user_input,
                    "pyspark":      last_code,
                    "results":      result,
                    "insights":     summary,
                    "text_results": text_results,
                    "attempts":     attempt,
                }

            last_error = execution.get("error", "Unknown execution error")
            logger.warning("Execution attempt %d failed: %s", attempt, last_error)

            if attempt < MAX_RETRIES:
                try:
                    corrected_code = self.codegen.correct(
                        question=user_input, context=context, failing_code=last_code, error_message=last_error
                    )
                    last_code = corrected_code
                except RuntimeError as e:
                    logger.warning("LLM correction failed on attempt %d: %s", attempt, e)
                    break

        return {
            "route":        "ADHOC",
            "format_bucket": format_bucket,
            "user_input":   user_input,
            "error":        last_error,
            "pyspark":      last_code,
            "results":      None,
            "insights":     None,
            "text_results": None,
            "attempts":     MAX_RETRIES,
        }
