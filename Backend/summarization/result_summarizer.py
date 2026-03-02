# core/summarizer/result_summarizer.py

from llm.llm_query import invoke_llm


class ResultSummarizer:
    """
    Generates structured, analytical business summaries for:
        - Query results
        - Dataset profiling
    """

    def summarize(self, question: str, result, mode: str = "query", max_rows: int = 5) -> str:
        if mode == "static":
            return self._summarize_profiling(result)
        return self._summarize_query(question, result, max_rows)

    # -----------------------------------------------------
    # QUERY MODE SUMMARY
    # -----------------------------------------------------
    def _summarize_query(self, question: str, result, max_rows: int) -> str:

        if not result or "rows" not in result or not result["rows"]:
            return "No data available to summarize."

        headers = result.get("columns", [])
        rows = result.get("rows", [])

        # Cap rows to avoid token overflow
        rows = rows[:max_rows]

        # Convert rows into list of dictionaries
        structured_rows = [
            dict(zip(headers, row))
            for row in rows
        ]

        data_text = f"(Columns: {headers}, Rows: {structured_rows})"

        prompt = f"""
            You are a Senior BI Analyst.

            Convert the DATA block into 4–6 concise analytical bullet points.

            INSTRUCTIONS:
            - Use only bullet points. No section headers.
            - Each bullet must begin with a short analytical label (e.g., "Workforce concentration:", "Compensation spread:", "Role clustering:").
            - Focus on distributions, outliers, ratios, dominance patterns, and structural trends.
            - Highlight key metrics or numbers in **bold**.
            - Italicize secondary comparisons where relevant.
            - Do not speculate. Do not hallucinate.
            - Do NOT mention Spark, PySpark, or technical implementation.

            USER QUESTION:
            {question}

            DATA:
            {data_text}
            """

        return invoke_llm(
            prompt=prompt,
            temperature=0.2,
            max_tokens=400
        )

    # -----------------------------------------------------
    # PROFILING MODE SUMMARY
    # -----------------------------------------------------
    def _summarize_profiling(self, profiling_result) -> str:

        if not profiling_result:
            return "No profiling data available."

        prompt = f"""
            You are a Senior Data Quality Analyst.

            Convert the PROFILING block into structured analytical bullet points.

            INSTRUCTIONS:
            - Use only bullet points (4–6 maximum).
            - Begin each bullet with a short diagnostic label (e.g., "Null exposure:", "Cardinality concentration:", "Metric dispersion:", "Schema volatility:").
            - Highlight key percentages or metrics in **bold**.
            - Identify high-null columns, extreme skew, or inconsistent formats.
            - Mention potential data quality risks clearly.
            - Do NOT mention Spark or implementation details.
            - Do not speculate beyond provided data.

            PROFILING DATA:
            {profiling_result}
            """

        return invoke_llm(
            prompt=prompt,
            temperature=0.2,
            max_tokens=450
        )