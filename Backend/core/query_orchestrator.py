# Backend/core/query_orchestrator.py

from logger.logger import get_logger
from config.settings import settings
from rag.kb_retriever import KnowledgeBaseRetriever
from rag.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.databricks_executor import DatabricksExecutor
from execution.emr_executor import EMRExecutor

logger = get_logger(__name__)


class QueryOrchestrator:
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()

        if settings.execution_mode == "databricks":
            self.executor = DatabricksExecutor()
        elif settings.execution_mode == "emr":
            self.executor = EMRExecutor()
        else:
            raise ValueError("Invalid execution mode")

    def run(self, question: str) -> dict:
        logger.info("User question: %s", question)

        chunks = self.retriever.retrieve(question)
        context = build_query_context(chunks)

        pyspark_code = self.codegen.generate(question, context)
        logger.info("Generated PySpark code:\n%s", pyspark_code)

        execution = self.executor.execute(context, pyspark_code)

        return {
            "pyspark_code": pyspark_code,
            "execution": execution,
        }
