def get_pyspark_prompt(columns: str, question: str) -> str:
    return f"""
You are generating PySpark DataFrame transformation code.

==============================
ABSOLUTE RULES (NO EXCEPTIONS)
==============================

- A DataFrame named `df` already exists.
- Use ONLY PySpark DataFrame APIs.
- Use `pyspark.sql.functions` strictly as `F`.
- NEVER import anything.
- NEVER use SQL strings or spark.sql().
- NEVER use groupBy unless the user explicitly asks for grouping.
- NEVER use crossJoin.
- NEVER create intermediate DataFrames.
- Output ONLY executable Python code.
- The final result MUST be a DataFrame named `final_df`.
- All string functions MUST be called via pyspark.sql.functions as F (e.g., F.upper(F.trim(F.col("x"))))
- NEVER call upper(), lower(), trim(), col() directly.
- ALWAYS use F.upper(F.trim(F.col("<column>")))

FUNCTION USAGE RULES (MANDATORY)

- ALL functions MUST be referenced via `F.<function>`.
- NEVER use bare functions like count(), col(), sum(), avg().
- Conditional counts MUST be written as:
  F.sum(F.when(condition, 1).otherwise(0))

==============================
SUMMARY RULE (VERY STRICT)
==============================

If the user asks for a "summary":

- Produce EXACTLY ONE `df.select(...)`
- Output MUST be a single-row DataFrame
- Allowed metrics ONLY:
  - total row count
  - distinct counts
  - conditional counts
  -FTE/CONSULTANTS
- DO NOT infer grouping columns
- DO NOT compute salary metrics unless explicitly requested
- DO NOT compute date min/max unless explicitly requested

==============================
SALARY HANDLING RULE
==============================

If salary metrics are explicitly requested:

- Salary columns are text-based
- NEVER operate on raw salary column
- ALWAYS create a derived numeric expression using:
  F.regexp_replace(col, '[^0-9.]', '')
- Safely cast to double before aggregation

=================================================================
For questions like "employee worked as manager in their career":
=================================================================
- Apply a single df.filter() that checks ALL role/title columns (current + previous) using case-insensitive matching.
- Always reference columns using F.col("<column>") and apply F.upper(F.trim(...)).contains("MANAGER"); never use col() directly.


==============================
ROLE / DESIGNATION MATCHING
==============================

- Matching must be case-insensitive
- Normalize using: F.upper(F.trim(F.col(col_name)))
- Career-wide role questions must check ALL role/title columns
- Use `contains()` after normalization

==============================
AVAILABLE COLUMNS
==============================
{columns}

==============================
USER QUESTION
==============================
{question}
"""
