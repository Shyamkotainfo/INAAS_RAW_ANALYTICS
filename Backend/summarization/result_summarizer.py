from llm.llm_query import invoke_llm


class ResultSummarizer:
    def summarize(self, question: str, rows: list[dict]) -> str:
        """
        Generate business-friendly insights from query results.
        """

        prompt = f"""
You are a senior data analyst.

User question:
{question}

Query result (sample rows):
{rows}

Generate:
1. A concise explanation of what the result shows.
2. Key insights or patterns.
3. Any notable trends or anomalies (if applicable).

Do NOT mention Spark, PySpark, code, or technical details.
Write in a clear, business-friendly tone.
"""

        return invoke_llm(
            prompt=prompt,
            temperature=0.2,
            max_tokens=300
        )
