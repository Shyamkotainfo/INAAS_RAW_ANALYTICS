import json
import boto3
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class DatasetOverviewGenerator:

    def __init__(self):
        self.client = boto3.client(
            "bedrock-runtime",
            region_name=settings.aws_region
        )
        self.model_id = settings.bedrock_model

    def generate(self, profiling: dict):

        try:

            # -----------------------------
            # Build reduced schema context
            # -----------------------------
            columns_context = []

            for col in profiling["columns"]:
                samples = [str(v) for v in col.get("sample_values", []) if v][:3]

                columns_context.append({
                    "name": col["column_name"],
                    "type": col["data_type"],
                    "examples": samples
                })

            llm_input = {
                "rows": profiling["row_count"],
                "columns": columns_context
            }

            prompt = f"""
You are a senior data analyst.

Analyze the dataset schema and generate a short dataset overview.

Rules:
- Return exactly 2 or 3 bullet points
- Do not invent columns
- Use column names and example values
- Keep it concise

Dataset schema:
{json.dumps(llm_input, indent=2)}

Return JSON only:

{{
  "overview": [
    "point 1",
    "point 2",
    "point 3"
  ]
}}
"""

            # -----------------------------
            # Nova Pro call
            # -----------------------------
            response = self.client.converse(
                modelId=self.model_id,
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"text": prompt}
                        ]
                    }
                ],
                inferenceConfig={
                    "maxTokens": 200,
                    "temperature": 0.2
                }
            )

            text_output = response["output"]["message"]["content"][0]["text"]

            overview_json = json.loads(text_output)

            return overview_json["overview"]

        except Exception as e:

            logger.exception("Dataset overview generation failed")

            return [
                "This dataset contains structured tabular records.",
                "It includes multiple attributes describing operational or transactional data.",
                "The dataset can be used for analytics, monitoring, or reporting."
            ]