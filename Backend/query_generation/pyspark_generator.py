# Backend/query_generation/pyspark_generator.py

from llm.llm_query import invoke_llm
from pyspark_utils.code_sanitizer import (
    strip_code_fences,
    rewrite_common_pyspark_imports,
)
from pyspark_utils.code_validator import validate_pyspark_code
from prompt.pyspark_code_gen import get_pyspark_prompt
from logger.logger import get_logger

logger = get_logger(__name__)


class PySparkCodeGenerator:
    def generate(self, question: str, context: dict) -> str:
        logger.info("Generating PySpark code from LLM")
        logger.debug(f"User question: {question}")

        # -----------------------------------------
        # STEP 1: Build column context
        # -----------------------------------------
        column_list = ", ".join(
            f"{c['name']} ({c['type']})" for c in context["columns"]
        )

        logger.debug(f"Available columns: {column_list}")

        # -----------------------------------------
        # STEP 2: Build prompt
        # -----------------------------------------
        prompt = get_pyspark_prompt(column_list, question)

        logger.debug("PySpark prompt constructed")
        logger.debug(f"Prompt preview (first 500 chars):\n{prompt[:500]}")

        # -----------------------------------------
        # STEP 3: Invoke LLM
        # -----------------------------------------
        raw_code = invoke_llm(prompt)

        logger.info("Received response from LLM")
        logger.debug(f"Raw LLM output:\n{raw_code}")

        # -----------------------------------------
        # STEP 4: Sanitize LLM output
        # -----------------------------------------
        code = strip_code_fences(raw_code)
        code = rewrite_common_pyspark_imports(code)

        logger.debug("Sanitized PySpark code")
        logger.debug(f"Sanitized code:\n{code}")

        # -----------------------------------------
        # STEP 5: Validate code
        # -----------------------------------------
        validate_pyspark_code(code)

        logger.info("PySpark code validated successfully")

        return code
