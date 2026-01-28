# Backend/summarization/result_summarizer.py

from bedrock.nova_client import NovaClient


class ResultSummarizer:
    def __init__(self):
        self.llm = NovaClient()

    def summarize(self, question: str, rows: list[dict]) -> str:
        prompt = f"""
User question:
{question}

Result rows (sample):
{rows}

Explain the result clearly and concisely.
"""

        return self.llm.generate(prompt, question)
