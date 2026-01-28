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
"""

        return self.llm.generate(prompt, question)

