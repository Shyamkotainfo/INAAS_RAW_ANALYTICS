# Backend/core/query_orchestrator.py

from logger.logger import get_logger
from rag.kb_retriever import KnowledgeBaseRetriever
from rag.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.databricks_executor import DatabricksExecutor
from summarization.result_summarizer import ResultSummarizer
from analytics.command_router import StaticCommandRouter


logger = get_logger(__name__)


class QueryOrchestrator:
    """
    Main orchestration engine for INAAS RAW mode.

    Supports:
        - Natural language queries (LLM → PySpark)
        - Static commands (@profiling, @quality)
    """

    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = DatabricksExecutor()
        self.summarizer = ResultSummarizer()

    def run(self, question: str) -> dict:
        logger.info("User question: %s", question)

        # --------------------------------------------------
        # Retrieve context (file path, schema, etc.)
        # --------------------------------------------------
        chunks = self.retriever.retrieve(question)
        context = build_query_context(chunks)

        # --------------------------------------------------
        # Generate PySpark
        # --------------------------------------------------
        if question.startswith("@"):
            # Static command mode (profiling / quality)
            router = StaticCommandRouter(question)
            pyspark_code = router.generate_pyspark()
            mode = "static"
        else:
            # Normal LLM → PySpark mode
            pyspark_code = self.codegen.generate(question, context)
            mode = "query"

        logger.info("Generated PySpark code:\n%s", pyspark_code)

        # --------------------------------------------------
        # Execute in Databricks
        # --------------------------------------------------
        execution = self.executor.execute(context, pyspark_code)

        if execution["status"] != "SUCCESS":
            return {
                "user_input": question,
                "pyspark": pyspark_code,
                "results": None,
                "insights": None,
                "error": execution.get("error"),
            }

        result = execution["result"]

        # --------------------------------------------------
        # Summarization
        # --------------------------------------------------
        summary = self.summarizer.summarize(
            question=question,
            result=result,
            mode=mode
        )

        # --------------------------------------------------
        # Final Response
        # --------------------------------------------------
        return {
            "user_input": question,
            "pyspark": pyspark_code,
            "results": result,     # FULL profiling JSON goes here
            "insights": summary,
        }
