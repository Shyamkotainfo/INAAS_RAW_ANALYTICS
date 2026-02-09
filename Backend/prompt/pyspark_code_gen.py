from prompt.base_rules import base_rules


def get_pyspark_prompt(columns: str, question: str) -> str:
    return f"""
You are generating PySpark transformation code.

Rules:
- A DataFrame named `df` already exists
- Use only the columns listed below
- Use pyspark.sql.functions as F
- Do NOT read files
- Do NOT create SparkSession
- Do NOT write data
- Output ONLY valid Python code
- Final output must be a DataFrame named `final_df`

Available columns:
{columns}

User question:
{question}

IMPORTANT:
- Do NOT use Markdown
- Do NOT include ``` or ```python
- Do NOT import any modules
- Use F.<function>() only
- NEVER use `.count()`
- For counting rows, ALWAYS use:
  final_df = df.select(F.count("*").alias("<name>"))
- For grouped counts, ALWAYS use:
  final_df = df.groupBy("<col>").agg(F.count("*").alias("<name>"))

Rules:
{base_rules}
"""
