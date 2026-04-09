from classifier.routing_schema import QueryRoute, ClassificationResult
from classifier.llm_classifier import LLMFallbackClassifier
from logger.logger import get_logger

logger = get_logger(__name__)

class QueryClassifier:
    """
    Routes natural language questions to the correct analytics layer via LLM Classification.
    Determines if a question is METADATA, PROFILE, KPI, or ADHOC.
    """

    def __init__(self):
        self.llm_classifier = LLMFallbackClassifier()

    def classify(self, question: str) -> ClassificationResult:
        """
        Main routing function:
        Directly uses the LLM to classify the user query.
        """
        q = question.lower().strip()
        logger.info("Classifying query via LLM: '%s'", q)
        
        result = self.llm_classifier.classify(q)
        
        logger.info(
            "Classification: Route=%s, Confidence=%.2f, Reasoning='%s'", 
            result.route, result.confidence, result.reasoning
        )
        return result
