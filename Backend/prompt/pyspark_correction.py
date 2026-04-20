# Backend/prompt/pyspark_correction.py


def get_correction_prompt(
    columns: str,
    question: str,
    failing_code: str,
    error_message: str,
    semantic_context: str | None = None,
    resolved_terms: dict[str, str] | None = None
) -> str:
    return f"""
You are an **Elite PySpark Debugging Agent**.

A PySpark query was generated to answer a user's question about raw data, but it FAILED when executed
on Databricks. Your task is to analyse the error and return a corrected, fully executable version.

=====================================================
ENVIRONMENT
=====================================================

A raw DataFrame named `df` already exists.
The environment provides ONLY:
  from pyspark.sql import functions as F
  from pyspark.sql import Window

You MUST NOT import anything else.
FINAL result MUST be assigned to exactly one variable named: final_df

=====================================================
AVAILABLE COLUMNS (exact names, base dataset only)
=====================================================

{columns}

=====================================================
SEMANTIC CONTEXT
=====================================================

{semantic_context or "No additional semantic context provided."}

SEMANTIC CONTEXT USAGE RULES:
- Treat SEMANTIC CONTEXT as business guidance only, not as additional schema.
- NEVER use a semantic concept as a DataFrame column unless that exact column exists in AVAILABLE COLUMNS.
- If a failing query used concepts like Department, Manager, Location, Business Unit, or compensation band as if they were real columns, remove those references unless the exact column exists.

=====================================================
RESOLVED TERM MAPPINGS
=====================================================

{resolved_terms or "No explicit term mappings resolved."}

RESOLUTION RULES:
- If the user used a business term that appears in RESOLVED TERM MAPPINGS, rewrite the code to use the mapped real column.
- Prefer resolved mappings over guessing new column names.

=====================================================
USER QUESTION
=====================================================

{question}

=====================================================
FAILING CODE
=====================================================

{failing_code}

=====================================================
EXECUTION ERROR
=====================================================

{error_message}

=====================================================
CORRECTION RULES
=====================================================

1. Read the error message carefully. Identify the ROOT CAUSE:
   - AnalysisException -> wrong column name, incompatible schema, wrong join key, bad cast
   - AttributeError -> method called on wrong object type (e.g. F.groupBy instead of df.groupBy)
   - ParseException -> syntax error in string expressions
   - IllegalArgumentException -> bad function arguments (e.g. wrong format string)
   - TypeError -> Python-level type mismatch

2. Fix ONLY the lines causing the error. Do NOT rewrite the entire logic unnecessarily.

3. ABSOLUTE RULES:
   - NEVER invent base dataset column names. Use ONLY the exact names in AVAILABLE COLUMNS for the raw input schema.
   - Derived columns created via `.alias(...)`, `.withColumn(...)`, aggregate aliases, or joins are valid AFTER they are introduced.
   - Semantic-layer concepts are not columns unless the exact column exists in AVAILABLE COLUMNS.
   - NEVER use spark.sql(). Only DataFrame APIs.
   - NEVER use python loops or list comprehensions.
   - groupBy is a DataFrame method. Use df.groupBy(...). NEVER F.groupBy(...).
   - DO NOT use df.count(). Use df.select(F.count("*").alias("total_rows")).
   - For unionByName, all intermediate DataFrames must have identical column structures.
   - Avoid crossJoin.

4. If the error is due to a missing base dataset column:
   - Remove that column reference entirely.
   - Substitute the closest semantically matching column from AVAILABLE COLUMNS, or omit the step.
   - Do NOT keep semantic-layer-only concepts such as Department if they are not present in AVAILABLE COLUMNS.

5. If the error is an AnalysisException about schema mismatch before unionByName:
   - Align ALL intermediate DataFrames to have exactly the same column names and types.

6. Compensation-specific fixes:
   - Treat `Pay` as text unless code explicitly normalizes it into a numeric derived column first.
   - Before compensation aggregation, ranking, filtering, min/max, variance, or percentile logic, normalize `Pay`.
   - Exclude null or unparseable normalized pay values from compensation math.
   - Do NOT treat "compensation band" as average compensation unless SEMANTIC CONTEXT explicitly defines a band model.
   - If SEMANTIC CONTEXT does not define compensation bands, prefer designation-level average compensation as the fallback comparison.

=====================================================
CHAIN OF THOUGHT (MANDATORY - use # comments)
=====================================================

Before writing corrected code, reason through:
# Root cause  : What exactly caused the error?
# Fix strategy : What specific line(s) need to change and why?
# Column check : Which references are base columns versus valid derived columns?
# Schema check : Are all DataFrames consistent before any union?

=====================================================
OUTPUT FORMAT
=====================================================

Return ONLY executable Python code.
Start with your # comments.
Then write the corrected PySpark code.
Do NOT include markdown block markers (e.g. ```python).
The final line MUST assign the result to: final_df
"""
