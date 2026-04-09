# Backend/classifier/routing_schema.py
# Enum and contract for query routing decisions.

from enum import Enum
from pydantic import BaseModel
from typing import Optional


class QueryRoute(str, Enum):
    METADATA = "METADATA"   # Layer 1 — schema/structural questions
    PROFILE  = "PROFILE"    # Layer 2 — distribution/stats questions
    KPI      = "KPI"        # Layer 3 — pre-built business metric questions
    ADHOC    = "ADHOC"      # Layer 4 — on-demand PySpark generation


class ClassificationResult(BaseModel):
    route: QueryRoute
    confidence: float           # 0.0 – 1.0
    resolved_kpi: Optional[str] = None   # Populated only when route == KPI
    reasoning: Optional[str] = None      # Optional LLM rationale
