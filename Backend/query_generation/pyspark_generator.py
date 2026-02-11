from logger.logger import get_logger
from llm.llm_query import LLMQuery
from prompt.pyspark_code_gen import get_pyspark_prompt

logger = get_logger(__name__)


class PySparkCodeGenerator:
    def __init__(self):
        self.llm = LLMQuery()

    def generate(self, question: str, context: dict) -> str:
        if "columns" not in context or not context["columns"]:
            raise RuntimeError("No columns available in query context")

        columns = ", ".join(c["name"] for c in context["columns"])
        prompt = get_pyspark_prompt(columns, question)

        logger.info("Sending PySpark generation prompt to LLM")
        code = self.llm.generate(prompt).strip()
        logger.info("Received PySpark code from LLM")

        code = self._sanitize(code)

        if "final_df" not in code:
            logger.error("Invalid PySpark code:\n%s", code)
            raise RuntimeError("Generated PySpark code missing final_df")

        if "crossJoin" in code:
            raise RuntimeError("crossJoin is not allowed")

        return code

    def _sanitize(self, code: str) -> str:
        cleaned = []
        for line in code.splitlines():
            line = line.strip()

            if not line:
                continue
            if line.startswith(("```", "from ", "import ", "#")):
                continue
            if line.lower().startswith(("here", "this code", "the following")):
                continue

            cleaned.append(line)

        return "\n".join(cleaned)
