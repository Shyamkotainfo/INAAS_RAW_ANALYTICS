# Query Classifier
# Routes NL questions to the correct layer:
#   METADATA  → metadata_store  (schema, grain, relationships)
#   PROFILE   → profile_store   (null counts, distributions, distinct values)
#   KPI       → semantic_layer  (headcount, avg pay, trends, diversity)
#   ADHOC     → on-demand PySpark generation (query_generation + execution)
