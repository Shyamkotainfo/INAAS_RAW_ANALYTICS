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
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = DatabricksExecutor()
        self.summarizer = ResultSummarizer()

    def run(self, question: str) -> dict:
        logger.info("User question: %s", question)

        # ---------------- Retrieve context ----------------
        chunks = self.retriever.retrieve(question)
        context = build_query_context(chunks)

        # ---------------- Generate PySpark ----------------
        # PROFILING MODE (Special Commands)
        if question.startswith("@"):
            router = StaticCommandRouter(question)
            pyspark_code = router.generate_pyspark()
        else:
            pyspark_code = self.codegen.generate(question, context)
        logger.info("Generated PySpark code:\n%s", pyspark_code)

        # ---------------- Execute ----------------
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

        # ---------------- Summarize ----------------
        # # Convert rows into dict format for summarizer
        # rows_as_dict = [
        #     dict(zip(result["columns"], row))
        #     for row in result["rows"]
        # ]

        summary = self.summarizer.summarize(
            question=question,
            result=result
        )

        # ---------------- Final JSON ----------------
        return {
            "user_input": question,
            "pyspark": pyspark_code,
            "results": result,
            "insights": summary,
        }
