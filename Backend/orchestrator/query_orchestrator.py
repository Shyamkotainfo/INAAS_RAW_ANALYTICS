# Backend/orchestrator/query_orchestrator.py

from retrieval.kb_retriever import KnowledgeBaseRetriever
from retrieval.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.local_pyspark_executor import LocalPySparkExecutor
from summarization.result_summarizer import ResultSummarizer


class QueryOrchestrator:
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = LocalPySparkExecutor()
        self.summarizer = ResultSummarizer()

    def run(self, question: str):
        chunks = self.retriever.retrieve(question)
        context = build_query_context(chunks)

        if not context.get("file_id"):
            raise RuntimeError("File path could not be resolved from KB retrieval")

        pyspark_code = self.codegen.generate(question, context)
        rows = self.executor.execute(context["file_id"], pyspark_code)

        return {
            "pyspark_code": pyspark_code,
            "result": rows,
            "summary": self.summarizer.summarize(question, rows)
        }

