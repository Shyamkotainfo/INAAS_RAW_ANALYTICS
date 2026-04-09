# Layer 2 — Profile / Statistics Store

## Purpose
Answers distribution and data-quality questions **without PySpark at query time**.

Stats are pre-computed once by `profiling_job.py` (a Databricks job) during dataset registration and stored as JSON. Reads are instant.

## What It Stores

| Stat | Columns |
|---|---|
| Null count + null % | All columns |
| Distinct value count | All columns |
| Is-unique flag | All columns |
| Min / Max | Numeric columns |
| Mean / Median / Stddev | Numeric columns |
| Top-10 most-frequent values | Categorical columns |
| Total row count | Per dataset |

## Questions It Can Answer

- "How many nulls are in the DOJ column?"
- "What is the pay range?"
- "What are the top 5 designations by headcount?"
- "Is EmpCode truly unique?"
- "What is the average age?"
- "What is the distribution of Gender?"

## Files

| File | Role |
|---|---|
| `profile_schema.py` | Pydantic models (`ColumnProfile`, `DatasetProfile`) |
| `profile_reader.py` | Reads pre-computed profile and formats NL answers |

## Databricks Job
`databricks_scripts/databricks/profiling_job.py` — runs once per dataset registration.

## Implementation Phase
**Phase B** — see `EXECUTION_PLAN.md`
