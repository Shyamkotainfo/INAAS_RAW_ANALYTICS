# Backend/classifier/embedding_service.py

import json
import boto3
from typing import List
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)

class EmbeddingService:
    """
    Connects to Amazon Bedrock to generate embeddings.
    """
    
    def __init__(self, model_id: str = "amazon.titan-embed-text-v2:0"):
        self.model_id = model_id
        # Use existing settings region
        self.client = boto3.client(
            service_name="bedrock-runtime",
            region_name=settings.aws_region
        )
        
    def get_embedding(self, text: str) -> List[float]:
        """
        Generates an embedding for the given text.
        """
        try:
            body = {"inputText": text}
            response = self.client.invoke_model(
                modelId=self.model_id,
                contentType="application/json",
                accept="application/json",
                body=json.dumps(body)
            )
            result = json.loads(response['body'].read())
            return result.get("embedding", [])
        except Exception as e:
            logger.error("Failed to generate embedding for text: '%s', error: %s", text, e)
            return []
