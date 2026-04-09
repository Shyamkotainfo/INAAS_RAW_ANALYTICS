# Layer 1 — Metadata Store

## Purpose
Answers schema/structural questions **without PySpark at query time**.

Metadata is stored once during dataset registration (`attach_file` flow) and read on demand by the query router.

## What It Stores

| Field | Description |
|---|---|
| Column names + data types | Full schema per dataset |
| Nullable flags | Whether a column allows nulls |
| Dimension vs measure | Categorical (grouping) vs numeric (aggregable) |
| Table grain | e.g. "one row per employee per month" |
| Table relationships | FK→PK in star/snowflake schemas |
| Business glossary | Plain-English definitions for domain terms |

## Questions It Can Answer

- "What columns does this table have?"
- "Which columns are dimensions vs measures?"
- "What is the grain of this dataset?"
- "What tables can be joined, and on which keys?"
- "What does 'Dummy Pay' mean?"

## Files

| File | Role |
|---|---|
| `metadata_schema.py` | Pydantic models (`ColumnMeta`, `DatasetMetadata`, `TableRelationship`) |
| `metadata_reader.py` | Reads stored metadata and formats NL answers |

## Implementation Phase
**Phase A** — see `EXECUTION_PLAN.md`
