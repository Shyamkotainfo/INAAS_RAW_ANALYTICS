from llm.llm_query import invoke_llm


class ResultSummarizer:
    """
    Generates business-friendly summaries for:
        - Normal query results
        - Profiling results
    """

    def summarize(self, question: str, result, mode: str = "query") -> str:
        """
        Generate business-friendly insights.
        """

        if mode == "static":
            return self._summarize_profiling(question, result)
        else:
            return self._summarize_query(question, result)

    # -----------------------------------------------------
    # Query Mode Summary
    # -----------------------------------------------------
    def _summarize_query(self, question: str, result) -> str:

        prompt = f"""
You are a senior data analyst.

User question:
{question}

Query result (sample result):
{result}

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

    # -----------------------------------------------------
    # Profiling Mode Summary
    # -----------------------------------------------------
    def _summarize_profiling(self, question: str, result) -> str:

        prompt = f"""
You are a senior data quality analyst.

The user requested dataset profiling.

Profiling output:
{result}

Generate:
1. Overall dataset health summary.
2. Columns with high null percentages.
3. Columns that may require data cleaning.
4. Any potential data quality risks.

Keep the explanation business-focused.
Do NOT mention Spark, PySpark, or technical implementation.
Be concise and structured.
"""

        return invoke_llm(
            prompt=prompt,
            temperature=0.2,
            max_tokens=350
        )
