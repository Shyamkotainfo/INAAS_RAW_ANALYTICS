from prompt.wiki_retriever import load_domain_semantic_context, retrieve_relevant_chunks


def get_pyspark_prompt(
    columns: str,
    question: str,
    semantic_context: str | None = None,
    resolved_terms: dict[str, str] | None = None
) -> str:
    pass


# def build_semantic_context(question: str, columns: str, top_k: int = 3) -> str:
def build_semantic_context(domain: str | None, question: str, columns: str, top_k: int = 1) -> str:
    """
    Optional supporting context only.
    This is allowed to provide glossary and narrative support, but it is not the
    primary semantic source for business-rule enforcement.
    """
    if domain and domain != "hr":
        return load_domain_semantic_context(domain)

    return retrieve_relevant_chunks(
        question=question,
        schema_columns=columns,
        top_k=top_k
    )


def _format_columns(columns: str) -> str:
    column_names = [column.strip() for column in columns.split(",") if column.strip()]
    return "\n".join(f"- {column}" for column in column_names) or "- None"


def _format_mappings(resolved_mappings: dict[str, str]) -> str:
    if not resolved_mappings:
        return "- None"

    lines = []
    for logical_name, physical_name in resolved_mappings.items():
        lines.append(f"- {logical_name} -> {physical_name}")
    return "\n".join(lines)


def _format_rule(business_rule: dict | None) -> str:
    if not business_rule:
        return "None. Use schema-only reasoning and do not invent business logic."

    lines = []
    rule_name = business_rule.get("name", "unknown_rule")
    lines.append(f"Rule name: {rule_name}")
    lines.append(f"Definition: {business_rule.get('definition', 'N/A')}")

    required_fields = business_rule.get("required_fields", [])
    if required_fields:
        lines.append("Required logical fields:")
        lines.extend(f"- {field}" for field in required_fields)

    required_any_of = business_rule.get("required_any_of", [])
    if required_any_of:
        lines.append("Require at least one of:")
        lines.extend(f"- {field}" for field in required_any_of)

    rule_steps = business_rule.get("rule", [])
    if rule_steps:
        lines.append("Rule steps:")
        lines.extend(f"- {step}" for step in rule_steps)

    voluntary_examples = business_rule.get("voluntary_examples", [])
    if voluntary_examples:
        lines.append("Voluntary examples:")
        lines.extend(f"- {value}" for value in voluntary_examples)

    involuntary_examples = business_rule.get("involuntary_examples", [])
    if involuntary_examples:
        lines.append("Involuntary examples:")
        lines.extend(f"- {value}" for value in involuntary_examples)

    fallback = business_rule.get("fallback")
    if fallback:
        lines.append(f"Fallback: {fallback}")

    missing_fields = business_rule.get("missing_required_fields", [])
    if missing_fields:
        lines.append("Missing required logical fields:")
        lines.extend(f"- {field}" for field in missing_fields)

    return "\n".join(lines)


def _format_supporting_context(semantic_context: str | None) -> str:
    if semantic_context and semantic_context.strip():
        return semantic_context.strip()
    return "None"


def get_pyspark_prompt(
    columns: str,
    question: str,
    resolved_mappings: dict[str, str] | None = None,
    business_rule: dict | None = None,
    guardrails: str | None = None,
    semantic_context: str | None = None,
) -> str:
    return f"""
You are generating executable PySpark code.

AVAILABLE COLUMNS:
{_format_columns(columns)}

RESOLVED COLUMN MAPPINGS (USE WHEN PRESENT):
{_format_mappings(resolved_mappings or {})}

BUSINESS RULE (USE WHEN PRESENT):
{_format_rule(business_rule)}

HARD GUARDRAILS:
{(guardrails or "None").strip()}

OPTIONAL SUPPORTING CONTEXT:
{_format_supporting_context(semantic_context)}

AVAILABLE HELPERS:
- as_text("column_name"): trimmed string value
- as_double("column_name"): numeric cast that handles values like "93,97"
- as_int("column_name"): integer cast built from as_double
- as_date("column_name"): date cast for yyyy-mm-dd and timestamp-like strings
- as_bool_flag("column_name"): boolean cast for TRUE/FALSE/1/0 text flags
- as_priority_rank("column_name"): maps High/Medium/Low text priority to 3/2/1

HARD RULES:
- NEVER invent column names.
- ONLY use exact columns from AVAILABLE COLUMNS.
- NEVER convert business concepts into fake columns.
- RESOLVED COLUMN MAPPINGS override your guesses when present.
- BUSINESS RULE must be applied exactly when present.
- If required fields are missing, return a one-row DataFrame with status = "CANNOT_COMPUTE" and a reason column.
- NEVER use SQL.
- ONLY use DataFrame API.
- When comparing, sorting, or aggregating string-encoded numeric/date/flag columns, use the helper functions first.
- NEVER redefine helper functions such as as_date, as_double, as_int, as_bool_flag, or as_priority_rank.
- If a priority column contains labels like High, Medium, Low, rank it with as_priority_rank instead of casting it to int.
- ALWAYS assign the final result to final_df.

MODE B — ANALYTICAL (everything else):
  Aggregations, trends, filters, top-N, distributions, comparisons, cohorts, anomalies.



def build_semantic_context(question: str, columns: str, top_k: int = 1) -> str:
    """
    Optional supporting context only.
    This is allowed to provide glossary and narrative support, but it is not the
    primary semantic source for business-rule enforcement.
    """
    return retrieve_relevant_chunks(
        question=question,
        schema_columns=columns,
        top_k=top_k
    )


def _format_columns(columns: str) -> str:
    column_names = [column.strip() for column in columns.split(",") if column.strip()]
    return "\n".join(f"- {column}" for column in column_names) or "- None"


def _format_mappings(resolved_mappings: dict[str, str]) -> str:
    if not resolved_mappings:
        return "- None"

    lines = []
    for logical_name, physical_name in resolved_mappings.items():
        lines.append(f"- {logical_name} -> {physical_name}")
    return "\n".join(lines)


def _format_rule(business_rule: dict | None) -> str:
    if not business_rule:
        return "None. Use schema-only reasoning and do not invent business logic."

    lines = []
    rule_name = business_rule.get("name", "unknown_rule")
    lines.append(f"Rule name: {rule_name}")
    lines.append(f"Definition: {business_rule.get('definition', 'N/A')}")

    required_fields = business_rule.get("required_fields", [])
    if required_fields:
        lines.append("Required logical fields:")
        lines.extend(f"- {field}" for field in required_fields)

    required_any_of = business_rule.get("required_any_of", [])
    if required_any_of:
        lines.append("Require at least one of:")
        lines.extend(f"- {field}" for field in required_any_of)

    rule_steps = business_rule.get("rule", [])
    if rule_steps:
        lines.append("Rule steps:")
        lines.extend(f"- {step}" for step in rule_steps)

    voluntary_examples = business_rule.get("voluntary_examples", [])
    if voluntary_examples:
        lines.append("Voluntary examples:")
        lines.extend(f"- {value}" for value in voluntary_examples)

    involuntary_examples = business_rule.get("involuntary_examples", [])
    if involuntary_examples:
        lines.append("Involuntary examples:")
        lines.extend(f"- {value}" for value in involuntary_examples)

    fallback = business_rule.get("fallback")
    if fallback:
        lines.append(f"Fallback: {fallback}")

    missing_fields = business_rule.get("missing_required_fields", [])
    if missing_fields:
        lines.append("Missing required logical fields:")
        lines.extend(f"- {field}" for field in missing_fields)

    return "\n".join(lines)


# 1. Mode    : META — Mode A
# 2. Type    : TYPE 5 — Data Quality Profiling
# 3. Columns : All columns from AVAILABLE COLUMNS
# 4. Cleaning: No cleaning needed — computing null counts directly.
# 5. Strategy: compute total_rows and null_count in one aggregation → derive null_pct → quality_flag.

final_df = df.select(
    F.count("*").alias("total_rows"),
    F.sum(F.when(F.col("Status").isNull(), 1).otherwise(0)).alias("null_count")
).select(
    F.lit("Status").alias("column_name"),
    F.col("null_count"),
    F.round(F.col("null_count") / F.col("total_rows") * 100, 2).alias("null_pct")
).withColumn("quality_flag", F.when(F.col("null_pct") > 20, "LOW_QUALITY")
                               .when(F.col("null_pct") > 5,  "MEDIUM_QUALITY")
                               .otherwise("HIGH_QUALITY"))

HARD GUARDRAILS:
{(guardrails or "None").strip()}

OPTIONAL SUPPORTING CONTEXT:
{_format_supporting_context(semantic_context)}

HARD RULES:
- NEVER invent column names.
- ONLY use exact columns from AVAILABLE COLUMNS.
- NEVER convert business concepts into fake columns.
- RESOLVED COLUMN MAPPINGS override your guesses when present.
- BUSINESS RULE must be applied exactly when present.
- If required fields are missing, return a one-row DataFrame with status = "CANNOT_COMPUTE" and a reason column.
- NEVER use SQL.
- ONLY use DataFrame API.
- ALWAYS assign the final result to final_df.

VALUE MATCHING RULES (CRITICAL):

- NEVER rely on exact string matches for categorical values.
- ALWAYS use case-insensitive matching using lower().

- For voluntary exits:
  match values containing:
    "resign", "voluntary"

- For involuntary exits:
  match values containing:
    "involuntary", "terminate"

- For retirement:
  match values containing:
    "retire"

- Example:
  Instead of:
    df["TerminationType"].isin(["Resigned"])
  Use:
    F.lower(F.col("TerminationType")).contains("resign")

- Combine conditions using OR where appropriate.

Before code, write only short # comments:
# 1. columns used
# 2. cleaning needed
# 3. transformation strategy

Return ONLY executable PySpark code.

USER QUESTION:
{question}
"""
