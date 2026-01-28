import json
import boto3
from typing import Optional
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


def get_llm_client(region: Optional[str] = None):
    """
    Create a Bedrock runtime client.
    """
    return boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1"
    )


def invoke_llm(
    prompt: str,
    model_id: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 800,
) -> str:
    """
    Invoke Amazon Bedrock Nova Pro and return raw text output.
    """

    model_id = model_id or settings.bedrock_model
    client = get_llm_client()

    logger.info(f"Invoking Bedrock model: {model_id}")

    response = client.invoke_model(
        modelId=model_id,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{ "text": prompt }]
                }
            ],
            "inferenceConfig": {
                "temperature": temperature,
                "maxTokens": max_tokens,
                "topP": 0.9
            }
        })
    )

    payload = json.loads(response["body"].read())
    return payload["output"]["message"]["content"][0]["text"]
