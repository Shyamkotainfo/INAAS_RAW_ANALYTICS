from logger.logger import get_logger
from llm.llm_query import LLMQuery
from prompt.pyspark_code_gen import get_pyspark_prompt

logger = get_logger(__name__)


class PySparkCodeGenerator:
    def __init__(self):
        self.llm = LLMQuery()

    def generate(self, question: str, context: dict) -> str:
        columns = ", ".join(c["name"] for c in context["columns"])
        prompt = get_pyspark_prompt(columns, question)

        logger.info("Sending PySpark generation prompt to LLM")

        code = self.llm.generate(prompt).strip()

        logger.info("Received PySpark code from LLM")

        if "final_df" not in code:
            logger.error("Generated code:\n%s", code)
            raise RuntimeError(
                "LLM failed to generate executable PySpark code "
                "(missing final_df)"
            )

        return code
