# Backend/query_generation/pyspark_generator.py

from llm.llm_query import invoke_llm
from pyspark_utils.code_sanitizer import strip_code_fences
from pyspark_utils.code_validator import validate_pyspark_code
from prompt.pyspark_generation import get_pyspark_prompt


class PySparkCodeGenerator:
    def generate(self, question: str, context: dict) -> str:
        column_list = ", ".join(
            f"{c['name']} ({c['type']})" for c in context["columns"]
        )

        prompt = get_pyspark_prompt(column_list, question)
        raw_code = invoke_llm(prompt)
        code = strip_code_fences(raw_code)
        validate_pyspark_code(code)
        return code
