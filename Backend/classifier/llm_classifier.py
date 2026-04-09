# Backend/classifier/llm_classifier.py

import json
from classifier.routing_schema import QueryRoute, ClassificationResult
from llm.llm_query import invoke_llm
from logger.logger import get_logger

logger = get_logger(__name__)

class LLMFallbackClassifier:
    """
    Acts as a fallback classifier using the LLM when embedding similarity is low.
    """
    
    def classify(self, question: str) -> ClassificationResult:
        prompt = f"""
Classify the following user query into exactly one of the following categories:
- METADATA: Queries about schema, structure, grain, available columns, physical relationships, and business glossary definitions (e.g. "What does Dummy Pay mean?").
- PROFILE: Queries about data quality, distributions, distinct values, missing nulls, row count, duplicates, minimums/maximums, top-N values.
- KPI: Queries about business metrics, aggregations, ratios, and trends (e.g., headcount by location, average salary, year over year growth, attrition trend).
- ADHOC: Queries requiring custom code, complex joins, deep drill-downs, filtering specific rows, or metrics not pre-built in typical KPIs.

You must return a STRICT JSON object in this exact format:
{{
  "route": "METADATA" | "PROFILE" | "KPI" | "ADHOC",
  "confidence": <float between 0.0 and 1.0>,
  "kpi_name": "<name of the KPI or null>",
  "reasoning": "<short explanation for why you chose this route>"
}}

User Query: "{question}"
"""
        
        try:
            response_text = invoke_llm(prompt=prompt, temperature=0.0, max_tokens=200)
            
            # Clean up the response if it has block quotes
            response_cleaned = response_text.replace("```json", "").replace("```", "").strip()
            result_dict = json.loads(response_cleaned)
            
            route_str = result_dict.get("route", "ADHOC").upper()
            try:
                route = QueryRoute(route_str)
            except ValueError:
                route = QueryRoute.ADHOC
                
            return ClassificationResult(
                route=route,
                confidence=float(result_dict.get("confidence", 0.5)),
                resolved_kpi=result_dict.get("kpi_name"),
                reasoning=result_dict.get("reasoning", "LLM classified")
            )
            
        except Exception as e:
            logger.error("LLM Fallback classification failed: %s", e)
            return ClassificationResult(
                route=QueryRoute.ADHOC,
                confidence=0.5,
                reasoning="LLM parsing failed; defaulting to ADHOC"
            )
