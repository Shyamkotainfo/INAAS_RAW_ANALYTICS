from logger.logger import get_logger
from llm.llm_query import invoke_llm

logger = get_logger(__name__)


class DatabricksSQLGenerator:
    def generate(self, question: str, context: dict) -> str:
        columns = ", ".join(c["name"] for c in context["columns"])
        file_path = context["file_path"]
        file_format = context["format"]

        prompt = f"""
You are generating Databricks SQL.

Context:
- Data is stored as raw files in cloud storage
- File path: {file_path}
- File format: {file_format}

Rules:
- Use Databricks SQL syntax
- Query files directly (no CREATE TABLE)
- Do NOT explain anything
- Output ONLY a single SQL query
- No markdown
- No comments

Available columns:
{columns}

User question:
{question}

Examples:
- For row count:
  SELECT COUNT(*) AS total_count FROM {file_format}.`{file_path}`;
- For column summary:
  SELECT MIN(col), MAX(col) FROM {file_format}.`{file_path}`;
"""

        logger.info("Sending SQL generation prompt to LLM")

        sql = invoke_llm(prompt).strip()

        if not sql.lower().startswith("select"):
            raise RuntimeError("LLM failed to generate valid SQL")

        return sql
