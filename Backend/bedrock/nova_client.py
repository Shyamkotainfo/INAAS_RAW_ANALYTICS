import json
import re
import boto3
from config.settings import settings


class NovaClient:
    """
    Amazon Bedrock Nova Pro client
    Generates PySpark transformation code using a fixed prompt.
    """

    def __init__(self):
        self.client = boto3.client(
            service_name="bedrock-runtime",
            region_name=settings.aws_region
        )

        self.model_id = settings.bedrock_model

    def generate(self, prompt: str) -> str:
        """
        Call Bedrock Nova Pro with the fixed prompt
        and return validated PySpark code.
        """

        response = self.client.invoke_model(
            modelId=self.model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps({
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            { "text": prompt }
                        ]
                    }
                ],
                "inferenceConfig": {
                    "temperature": 0.0,
                    "maxTokens": 800,
                    "topP": 0.9
                }
            })
        )

        payload = json.loads(response["body"].read())

        # Nova response format
        code = payload["output"]["message"]["content"][0]["text"].strip()
        code = self._strip_code_fences(code)
        self._validate_pyspark_code(code)

        return code


    def _strip_code_fences(self, code: str) -> str:
        """
        Remove ```python ``` wrappers if LLM returns Markdown
        """
        code = code.strip()

        if code.startswith("```"):
            code = code.split("```")[1]

        if code.endswith("```"):
            code = code.rsplit("```", 1)[0]

        return code.strip()


    # ------------------------------------------------------------------
    # SAFETY & CONTRACT VALIDATION
    # ------------------------------------------------------------------

    def _validate_pyspark_code(self, code: str):
        """
        Enforce strict PySpark execution rules.
        """

        forbidden_patterns = [
            r"import\s+",
            r"spark\.read",
            r"SparkSession",
            r"\.write\(",
            r"\.save\(",
            r"\.collect\(",
            r"\.count\(\)",  # scalar count
            r"open\(",
            r"subprocess",
            r"eval\(",
        ]

        for pattern in forbidden_patterns:
            if re.search(pattern, code):
                raise RuntimeError(
                    f"Unsafe PySpark code generated.\n"
                    f"Forbidden pattern: `{pattern}`\n\n{code}"
                )

        if "result_df" not in code:
            raise RuntimeError(
                "Generated PySpark code must define `result_df`"
            )
