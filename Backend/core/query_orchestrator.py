# Backend/core/query_orchestrator.py

from logger.logger import get_logger
from config.settings import settings
from rag.kb_retriever import KnowledgeBaseRetriever
from rag.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.databricks_executor import DatabricksExecutor

logger = get_logger(__name__)


class QueryOrchestrator:
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = DatabricksExecutor()

    def run(self, question: str) -> dict:
        logger.info("User question: %s", question)

        chunks = self.retriever.retrieve(question)
        if not chunks:
            raise RuntimeError("No KB context retrieved")

        context = build_query_context(chunks)

        pyspark_code = self.codegen.generate(question, context)
        logger.info("Generated PySpark code:\n%s", pyspark_code)

        execution = self.executor.execute(context, pyspark_code)

        return {
            "pyspark_code": pyspark_code,
            "execution": execution,
        }