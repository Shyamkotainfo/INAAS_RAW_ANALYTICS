from rag.kb_retriever import KnowledgeBaseRetriever
from rag.context_builder import build_query_context
from query_generation.pyspark_generator import PySparkCodeGenerator
from execution.local_pyspark_executor import LocalPySparkExecutor
# from summarization.result_summarizer import ResultSummarizer
from profiling.dataframe_profiler import DataFrameProfiler


class QueryOrchestrator:
    def __init__(self):
        self.retriever = KnowledgeBaseRetriever()
        self.codegen = PySparkCodeGenerator()
        self.executor = LocalPySparkExecutor()
        # self.summarizer = ResultSummarizer()

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

            # Load DataFrame ONLY (no LLM-generated code)
            df = self.executor.load_df(file_id)

            # Run deterministic auto-profiling
            profile = DataFrameProfiler(df).run()

            # Let LLM explain the profile
            # summary = self.summarizer.summarize_profile(profile)

            return {
                "mode": "profile",
                "file_id": file_id,
                "profile": profile,
                # "summary": summary
            }

        # -------------------------------------------
        # STEP 3: NORMAL QUERY MODE
        # -------------------------------------------
        pyspark_code = self.codegen.generate(question, context)
        rows = self.executor.execute(file_id, pyspark_code)

        return {
            "mode": "query",
            "pyspark_code": pyspark_code,
            "result": rows,
            # "summary": self.summarizer.summarize(question, rows[:10])
        }
