import json
from typing import Any, Optional

import boto3

from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class LLMInvocationError(RuntimeError):
    """Raised when the Bedrock model call fails."""

    def __init__(
        self,
        message: str,
        *,
        model_id: str,
        error_type: str = "llm_error",
    ) -> None:
        self.model_id = model_id
        self.error_type = error_type
        super().__init__(message)


def _classify_llm_error(exc: Exception) -> str:
    text = str(exc).lower()
    moderation_markers = (
        "guardrail",
        "content filter",
        "content filters",
        "moderation",
        "unsafe",
        "safety",
        "blocked",
    )
    if any(marker in text for marker in moderation_markers):
        return "moderation_blocked"
    return "llm_error"


def _extract_bedrock_text(payload: dict[str, Any], model_id: str) -> str:
    output = payload.get("output") or {}
    message = output.get("message") or {}
    content = message.get("content") or []
    if not content:
        raise LLMInvocationError(
            "Bedrock response did not contain any generated content.",
            model_id=model_id,
            error_type="llm_empty_response",
        )
    first_chunk = content[0] or {}
    text = first_chunk.get("text")
    if not isinstance(text, str) or not text.strip():
        raise LLMInvocationError(
            "Bedrock response did not contain generated text.",
            model_id=model_id,
            error_type="llm_empty_response",
        )
    return text


def get_llm_client(region: Optional[str] = None):
    return boto3.client(
        service_name="bedrock-runtime",
        region_name=region or settings.aws_region
    )


def invoke_llm(
    prompt: str,
    model_id: Optional[str] = None,
    temperature: float = 0.0,
    max_tokens: int = 800,
) -> str:
    model_id = model_id or settings.bedrock_model
    client = get_llm_client()

    logger.info(
        "Invoking Bedrock model | model=%s | prompt_chars=%d | temperature=%.2f | max_tokens=%d",
        model_id,
        len(prompt),
        temperature,
        max_tokens,
    )

    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ],
        "inferenceConfig": {
            "temperature": temperature,
            "maxTokens": max_tokens,
            "topP": 0.9
        }
    }

    try:
        response = client.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(request_body)
        )

        payload = json.loads(response["body"].read())
        text = _extract_bedrock_text(payload, model_id)
        logger.info(
            "Bedrock response received | model=%s | stop_reason=%s | output_chars=%d | moderation_result=%s",
            model_id,
            payload.get("stopReason"),
            len(text),
            payload.get("guardrailAction") or "none",
        )
        return text
    except LLMInvocationError:
        logger.exception("Bedrock response parsing failed | model=%s", model_id)
        raise
    except Exception as exc:
        error_type = _classify_llm_error(exc)
        logger.exception(
            "Bedrock invocation failed | model=%s | error_type=%s",
            model_id,
            error_type,
        )
        raise LLMInvocationError(
            str(exc),
            model_id=model_id,
            error_type=error_type,
        ) from exc


# ✅ REQUIRED CLASS (this is what was missing)
class LLMQuery:
    def __init__(self):
        self.model_id = settings.bedrock_model

    def generate(
        self,
        prompt: str,
        temperature: float = 0.0,
        max_tokens: int = 800,
    ) -> str:
        return invoke_llm(
            prompt=prompt,
            model_id=self.model_id,
            temperature=temperature,
            max_tokens=max_tokens,
        )
