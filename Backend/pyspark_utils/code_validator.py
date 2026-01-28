import re


FORBIDDEN_PATTERNS = [
    r"import\s+",
    r"spark\.read",
    r"SparkSession",
    r"\.write\(",
    r"\.save\(",
    r"\.collect\(",
    r"\.count\(\)",   # scalar count
    r"open\(",
    r"subprocess",
    r"eval\(",
]


def validate_pyspark_code(code: str):
    """
    Enforce strict PySpark execution rules.
    """

    for pattern in FORBIDDEN_PATTERNS:
        if re.search(pattern, code):
            raise RuntimeError(
                f"Unsafe PySpark code generated.\n"
                f"Forbidden pattern: `{pattern}`\n\n{code}"
            )

    if "result_df" not in code:
        raise RuntimeError(
            "Generated PySpark code must define `result_df`"
        )

    if "groupBy()" in code:
        raise RuntimeError(
            "Invalid PySpark code: groupBy() must include columns"
        )
