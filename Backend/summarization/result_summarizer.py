# core/summarizer/result_summarizer.py

from llm.llm_query import invoke_llm


class ResultSummarizer:
    """
    Generates structured, analytical raw data insights for:
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
            return "No data available to derive raw data insights."

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
            You are a Senior Raw Data Insights Analyst.

            Convert the DATA block into 4-6 concise raw data insight bullets.

            INSTRUCTIONS:
            - Use only bullet points. No section headers.
            - Each bullet must begin with a short insight label (e.g., "Pattern observed:", "Data concentration:", "Outlier signal:", "Category dominance:", "Distribution shift:").
            - Focus on raw data insights such as distributions, outliers, ratios, dominance patterns, concentration, spread, anomalies, missingness signals, and structural trends visible in the result.
            - Explain what the result suggests about the raw data, not just what the table literally contains.
            - Highlight key metrics or numbers in **bold**.
            - Do not speculate. Do not hallucinate.
            - Do not describe this as a summary.
            - Do NOT mention Spark, PySpark, or technical implementation.
            - If the result is aggregated, explain the main business or data signal from the aggregation.
            - If the result is record-level, point out notable patterns, unusual values, or data quality signals only if they are visible in the result.

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
            return "No profiling data available to derive raw data insights."

        prompt = f"""
            You are a Senior Raw Data Profiling Analyst.

            Convert the PROFILING block into structured raw data insight bullets.

            INSTRUCTIONS:
            - Use only bullet points (4–6 maximum).
            - Begin each bullet with a short insight label (e.g., "Null exposure:", "Cardinality signal:", "Metric spread:", "Schema pattern:", "Quality risk:").
            - Focus on raw data characteristics and what they imply for downstream analysis.
            - Highlight key percentages or metrics in **bold**.
            - Identify high-null columns, extreme skew, or inconsistent formats.
            - Mention potential data quality risks clearly, but frame them as raw data insights rather than generic summaries.
            - Do not describe this as a summary.
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
