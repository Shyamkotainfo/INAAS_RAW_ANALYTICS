# Backend/summarization/result_summarizer.py
from llm.llm_query import invoke_llm


class ResultSummarizer:
    def summarize(self, question: str, rows: list[dict]) -> str:
        """
        Generate a natural-language summary of query results.
        """

        prompt = f"""
You are a data analyst assistant.

User question:
{question}

Query result (sample rows):
{rows}

Provide a clear, concise, business-friendly explanation of the result.
Do NOT mention technical details like Spark, DataFrames, or code.
"""

        return invoke_llm(
            prompt=prompt,
            temperature=0.2,
            max_tokens=300
        )
