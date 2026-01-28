# Backend/rag/kb_retriever.py

import boto3
from config.settings import settings
from logger.logger import get_logger

logger = get_logger(__name__)


class KnowledgeBaseRetriever:
    def __init__(self):
        self.client = boto3.client(
            "bedrock-agent-runtime",
            region_name=settings.aws_region
        )

    def retrieve(self, question: str, top_k: int = 5) -> list[str]:
        response = self.client.retrieve(
            knowledgeBaseId=settings.bedrock_kb_id,
            retrievalQuery={"text": question},
            retrievalConfiguration={
                "vectorSearchConfiguration": {
                    "numberOfResults": top_k
                }
            }
        )

        chunks = [
            item["content"]["text"]
            for item in response.get("retrievalResults", [])
        ]

        logger.info(f"Retrieved {len(chunks)} KB chunks")
        return chunks
