# Backend/core/query_orchestrator.py

from rag.kb_retriever import KnowledgeBaseRetriever
from rag.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.executor_factory import get_executor
from summarization.result_summarizer import ResultSummarizer
from profiling.dataframe_profiler import DataFrameProfiler
from quality.quality_engine import run_quality_checks


class QueryOrchestrator:
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = get_executor()   # 🔥 INFRA SWITCH POINT
        self.summarizer = ResultSummarizer()

    def run(self, question: str):

        question = question.strip()

        # -------------------------------------------
        # STEP 1: Retrieve schema / file context
        # -------------------------------------------
        chunks = self.retriever.retrieve(question)
        context = build_query_context(chunks)

        if not context.get("file_id"):
            raise RuntimeError("File path could not be resolved from KB RAG")

        file_id = context["file_id"]

        # -------------------------------------------
        # STEP 2: PROFILE MODE (@profile)
        # -------------------------------------------
        if question.lower().startswith("@profile"):

            df = self.executor.load_df(file_id)
            profile = DataFrameProfiler(df).run()
            summary = self.summarizer.summarize_profile(profile)

            return {
                "mode": "profile",
                "file_id": file_id,
                "profile": profile,
                "summary": summary
            }

        # -------------------------------------------
        # STEP 3: QUALITY MODE (@quality)
        # -------------------------------------------
        if question.lower().startswith("@quality"):

            df = self.executor.load_df(file_id)
            quality_report = run_quality_checks(df)
            summary = self.summarizer.summarize_profile(quality_report)

            return {
                "mode": "quality",
                "file_id": file_id,
                "quality": quality_report,
                "summary": summary
            }

        # -------------------------------------------
        # STEP 4: NORMAL QUERY MODE
        # -------------------------------------------
        pyspark_code = self.codegen.generate(question, context)
        rows = self.executor.execute_query(file_id, pyspark_code)

        return {
            "mode": "query",
            "pyspark_code": pyspark_code,
            "result": rows,
            "summary": self.summarizer.summarize(question, rows[:10])
        }
