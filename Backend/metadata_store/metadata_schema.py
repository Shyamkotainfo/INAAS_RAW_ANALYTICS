# Backend/metadata_store/metadata_schema.py
# Pydantic models for the Layer 1 Metadata Store contracts.

from typing import List, Optional
from pydantic import BaseModel


class ColumnMeta(BaseModel):
    name: str
    dtype: str
    nullable: bool
    is_dimension: bool        # True for categorical/grouping columns
    is_measure: bool          # True for numeric/aggregable columns
    business_description: Optional[str] = None  # From business glossary


class TableRelationship(BaseModel):
    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_type: str    # e.g. "FK->PK", "many-to-one"


class DatasetMetadata(BaseModel):
    dataset_id: str
    file_path: str
    file_format: str
    table_grain: Optional[str] = None   # e.g. "one row per employee per month"
    columns: List[ColumnMeta]
    relationships: List[TableRelationship] = []
    business_glossary: dict = {}         # {"term": "definition", ...}
    registered_at: Optional[str] = None
