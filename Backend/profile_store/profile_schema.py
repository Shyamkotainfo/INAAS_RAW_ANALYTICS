# Backend/profile_store/profile_schema.py
# Pydantic models for the Layer 2 Profile / Statistics Store contracts.

from typing import List, Optional, Dict, Any
from pydantic import BaseModel


class ColumnProfile(BaseModel):
    column_name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    is_unique: bool
    # Numeric stats (populated only for numeric columns)
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    stddev: Optional[float] = None
    # Categorical stats (populated only for string/categorical columns)
    top_values: Optional[List[Dict[str, Any]]] = None  # [{"value": "X", "count": N}, ...]


class DatasetProfile(BaseModel):
    dataset_id: str
    total_rows: int
    total_columns: int
    profiled_at: Optional[str] = None
    columns: List[ColumnProfile]
