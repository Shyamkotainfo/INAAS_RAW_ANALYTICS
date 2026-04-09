# Backend/semantic_layer/kpi_schema.py
# Pydantic models for the Layer 3 Semantic / KPI Layer contracts.

from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class KPIResult(BaseModel):
    kpi_name: str               # e.g. "avg_pay_by_designation"
    dimensions: List[str]       # e.g. ["designation", "gender"]
    metric: str                 # e.g. "avg_pay"
    data: List[Dict[str, Any]]  # Resulting rows
    computed_at: Optional[str] = None
    source_table: Optional[str] = None  # Gold table name or S3 path


class KPICatalog(BaseModel):
    """
    Registry of all pre-built KPIs available in the semantic layer.
    Loaded at startup to help the classifier identify KPI questions.
    """
    kpis: List[str]  # e.g. ["headcount_by_designation", "avg_pay_by_role", ...]
