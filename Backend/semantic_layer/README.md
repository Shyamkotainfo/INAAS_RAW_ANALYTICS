# Layer 3 — Semantic / KPI Layer

## Purpose
Answers standard business metric questions **using SQL on pre-aggregated gold tables** — no PySpark at query time.

Gold tables are built once by `semantic_build_job.py` (a Databricks job). The KPI reader queries them with lightweight SQL.

## Pre-built KPIs

| KPI Name | Dimensions | Metric |
|---|---|---|
| `headcount_by_designation` | Designation | Headcount |
| `headcount_by_gender` | Gender | Headcount |
| `headcount_by_department` | Department | Headcount |
| `avg_pay_by_designation` | Designation | Avg Pay |
| `avg_pay_by_gender` | Gender | Avg Pay |
| `hiring_trend_monthly` | Hire Month | New Hires |
| `gender_ratio_by_department` | Department, Gender | Ratio % |

## Questions It Can Answer

- "What is avg pay for Sr. DevOps Engineers?"
- "How has headcount grown YoY?"
- "What is the gender ratio in tech roles?"
- "Which department has the highest hiring rate?"
- "Show me monthly hiring trend for 2024."

## Files

| File | Role |
|---|---|
| `kpi_schema.py` | Pydantic models (`KPIResult`, `KPICatalog`) |
| `kpi_reader.py` | Reads gold tables and formats NL answers |

## Databricks Job
`databricks_scripts/databricks/semantic_build_job.py` — runs once (or on schedule).

## Implementation Phase
**Phase C** — see `EXECUTION_PLAN.md`
