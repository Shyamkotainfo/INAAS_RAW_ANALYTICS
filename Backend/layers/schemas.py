"""
Pydantic contracts for layer payloads.

These are kept with the active layer runtime code so contracts evolve
with handlers/orchestrator together.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class ColumnMeta(BaseModel):
    name: str
    dtype: str
    nullable: bool
    is_dimension: bool
    is_measure: bool
    business_description: Optional[str] = None


class TableRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str


class DatasetMetadata(BaseModel):
    dataset_id: str
    file_path: str
    file_format: str
    table_grain: Optional[str] = None
    columns: List[ColumnMeta]
    relationships: List[TableRelationship] = []
    business_glossary: Dict[str, str] = {}
    registered_at: Optional[str] = None


class ColumnProfile(BaseModel):
    column_name: str
    dtype: str
    null_count: int
    null_pct: float
    distinct_count: int
    is_unique: bool
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    mean: Optional[float] = None
    median: Optional[float] = None
    stddev: Optional[float] = None
    top_values: Optional[List[Dict[str, Any]]] = None


class DatasetProfile(BaseModel):
    dataset_id: str
    total_rows: int
    total_columns: int
    profiled_at: Optional[str] = None
    columns: List[ColumnProfile]


class KPIResult(BaseModel):
    kpi_name: str
    dimensions: List[str]
    metric: str
    data: List[Dict[str, Any]]
    computed_at: Optional[str] = None
    source_table: Optional[str] = None


class KPICatalog(BaseModel):
    kpis: List[str]

