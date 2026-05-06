def strip_code_fences(code: str) -> str:
    """
    Remove markdown ``` wrappers from LLM output.
    """
    code = code.strip()

    if code.startswith("```"):
        code = code.split("```", 1)[1]

    if code.endswith("```"):
        code = code.rsplit("```", 1)[0]

    return code.strip()

import re

def rewrite_common_pyspark_imports(code: str) -> str:
    """
    Rewrite unsafe PySpark patterns into sandbox-safe equivalents.
    """

    # Remove pyspark.sql.functions imports
    code = re.sub(
        r"from\s+pyspark\.sql\.functions\s+import\s+\w+.*\n?",
        "",
        code
    )
    code = re.sub(
        r"from\s+pyspark\.sql\s+import\s+functions\s+as\s+F\s*\n?",
        "",
        code
    )

    # Replace bare col(...) with F.col(...)
    # BUT avoid double replacement
    code = re.sub(
        r"(?<!F\.)\bcol\(",
        "F.col(",
        code
    )

    # Fix accidental double prefix: F.F.col → F.col
    code = re.sub(
        r"\bF\.F\.col\(",
        "F.col(",
        code
    )

    return code.strip()


def strip_helper_redefinitions(code: str) -> str:
    """
    Remove LLM-defined helper functions that shadow runtime-provided helpers.
    """
    helper_names = [
        "as_text",
        "as_double",
        "as_int",
        "as_date",
        "as_bool_flag",
        "as_priority_rank",
    ]
    for helper_name in helper_names:
        code = re.sub(
            rf"(?ms)^def\s+{helper_name}\s*\([^)]*\):\n(?:^[ \t].*\n|^\n)*",
            "",
            code
        )
    return code.strip()
    
def sanitize(code: str) -> str:
    code = strip_code_fences(code)
    code = rewrite_common_pyspark_imports(code)
    code = strip_helper_redefinitions(code)
    return code.strip()
