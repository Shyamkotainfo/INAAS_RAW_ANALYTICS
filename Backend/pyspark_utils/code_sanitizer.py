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

