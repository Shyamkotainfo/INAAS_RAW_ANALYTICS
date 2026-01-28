# Backend/query_generation/pyspark_generator.py

from bedrock.nova_client import NovaClient



class PySparkCodeGenerator:
    def __init__(self):
        self.llm = NovaClient()

    def generate(self, question: str, context: dict) -> str:
        column_list = ", ".join(
            f"{c['name']} ({c['type']})" for c in context["columns"]
        )

        prompt = f"""
You are generating PySpark transformation code.

Rules:
- A DataFrame named `df` already exists
- Use only the columns listed below
- Use pyspark.sql.functions as F
- Do NOT read files
- Do NOT create SparkSession
- Do NOT write data
- Output ONLY valid Python code
- Final output must be a DataFrame named `result_df`

Available columns:
{column_list}

User question:
{question}

IMPORTANT:
- Do NOT use Markdown
- Do NOT include ``` or ```python
- Do NOT import any modules
- Use F.<function>() only
- NEVER use `.count()`
- For counting rows, ALWAYS use:
  result_df = df.select(F.count("*").alias("<name>"))
- For grouped counts, ALWAYS use:
  result_df = df.groupBy("<col>").agg(F.count("*").alias("<name>"))

"""

        return self.llm.generate(prompt)

