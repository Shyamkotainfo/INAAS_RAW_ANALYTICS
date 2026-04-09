# Backend/classifier/query_classifier.py

from typing import Dict, List, Tuple
from classifier.routing_schema import QueryRoute, ClassificationResult
from classifier.embedding_service import EmbeddingService
from classifier.similarity import SimilarityScorer
from classifier.llm_classifier import LLMFallbackClassifier
from logger.logger import get_logger

logger = get_logger(__name__)

# Predefined examples including user's specific domains
CLASS_EXAMPLES = {
    QueryRoute.METADATA: [
        "what columns exist",
        "schema of table",
        "what is the grain of dataset",
        "what are dimensions and measures",
        "what are join keys",
        "what hierarchies exist",
        "data types of columns",
        "What are the probable dimensions of this dataset?",
        "What is the grain of this dataset?",
        "What are the foreign keys or join columns?",
        "What hierarchies exist (Country → State → City)?",
        "What are the data types of each column?"
    ],
    QueryRoute.PROFILE: [
        "null values per column",
        "distribution of age",
        "top 10 values",
        "row count",
        "data quality issues",
        "duplicates in dataset",
        "range of values",
        "What is the null rate per column?",
        "Are there duplicate rows?",
        "What is the distribution of values?",
        "What is the range of numeric columns?",
        "How many rows exist?"
    ],
    QueryRoute.KPI: [
        "average salary by department",
        "attrition trend",
        "headcount by location",
        "monthly growth",
        "gender ratio",
        "revenue trend",
        "year over year growth",
        "What KPIs can be derived from this dataset?",
        "What is the average salary by department?",
        "What is the attrition trend?",
        "What is the monthly growth?"
    ]
}

# Global cache so we don't recreate embeddings every time QueryClassifier is instantiated per request.
_GLOBAL_EMBEDDING_CACHE: Dict[QueryRoute, List[List[float]]] = {}

class QueryClassifier:
    """
    Routes natural language questions to the correct analytics layer via Semantic Similarity.
    Fallback to Amazon Bedrock LLM if not confident.
    """

    def __init__(self):
        self.embedding_service = EmbeddingService()
        self.llm_classifier = LLMFallbackClassifier()
        self.confidence_threshold = 0.65
        self._ensure_cache()

    def _ensure_cache(self):
        """Builds embedding cache on startup to minimize latency."""
        global _GLOBAL_EMBEDDING_CACHE
        if _GLOBAL_EMBEDDING_CACHE:
            return

        logger.info("Building semantic routing embedding cache...")
        new_cache = {route: [] for route in CLASS_EXAMPLES.keys()}
        
        for route, examples in CLASS_EXAMPLES.items():
            for text in set(examples):  # deduplicate examples
                emb = self.embedding_service.get_embedding(text)
                if emb:
                    new_cache[route].append(emb)

        if any(new_cache.values()):  # Only save if we got valid embeddings
            _GLOBAL_EMBEDDING_CACHE.update(new_cache)
            logger.info("Semantic routing cache built successfully.")
        else:
            logger.warning("Failed to build any embeddings for the semantic cache.")

    def classify(self, question: str) -> ClassificationResult:
        """
        Main routing function:
        1. Embed the query
        2. Compute similarity against anchored layers
        3. Threshold evaluation -> Return or Fallback to LLM
        """
        q = question.lower().strip()
        
        # Step 1: Embedding-Based Classification
        query_embedding = self.embedding_service.get_embedding(q)
        
        if query_embedding and _GLOBAL_EMBEDDING_CACHE:
            best_route, best_score = self._compute_best_semantic_match(query_embedding)
            
            logger.info("Semantic Matching - Best Route: %s, Score: %.3f", best_route, best_score)
            
            if best_score >= self.confidence_threshold:
                return ClassificationResult(
                    route=best_route,
                    confidence=best_score,
                    reasoning=f"Semantic threshold met (Score: {best_score:.3f})"
                )

        # Step 2: LLM Fallback
        logger.info("Semantic score below threshold (%.2f), falling back to LLM...", self.confidence_threshold)
        return self.llm_classifier.classify(q)

    def _compute_best_semantic_match(self, query_emb: List[float]) -> Tuple[QueryRoute, float]:
        """Iterate all anchored vectors and return route with highest cosine similarity."""
        best_score = 0.0
        best_route = QueryRoute.ADHOC

        for route, anchor_embeddings in _GLOBAL_EMBEDDING_CACHE.items():
            route_max_score = 0.0
            for anchor_emb in anchor_embeddings:
                score = SimilarityScorer.cosine_similarity(query_emb, anchor_emb)
                if score > route_max_score:
                    route_max_score = score
                    
            if route_max_score > best_score:
                best_score = route_max_score
                best_route = route

        return best_route, best_score
