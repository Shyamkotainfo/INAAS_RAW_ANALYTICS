# Query Classifier

## Purpose
Routes natural language questions to the correct analytics layer **before** any PySpark generation is attempted.

By routing ~60–70% of typical business questions away from code generation, the system becomes faster, cheaper, and more reliable.

## Routing Logic

```
NL Question
    ↓
QueryClassifier.classify()
    ├── METADATA → metadata_store   (schema, grain, glossary)
    ├── PROFILE  → profile_store    (nulls, distributions, top-N)
    ├── KPI      → semantic_layer   (headcount, avg pay, trends)
    └── ADHOC    → query_generation + execution (on-demand PySpark)
```

## Two-Tier Approach

1. **Rule-based fast-path** (no LLM, instant) — regex keyword signals per layer
2. **LLM fallback** (low-latency classifier prompt) — used when rule confidence < threshold

## Files

| File | Role |
|---|---|
| `routing_schema.py` | `QueryRoute` enum + `ClassificationResult` model |
| `query_classifier.py` | `QueryClassifier` class with `classify()` method |

## Implementation Phase
**Phase D** — see `EXECUTION_PLAN.md`
