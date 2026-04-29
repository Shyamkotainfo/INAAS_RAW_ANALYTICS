from prompt.wiki_retriever import retrieve_relevant_chunks


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
        return "No exact business rule matched the question."

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

RESOLVED COLUMN MAPPINGS (MANDATORY):
{_format_mappings(resolved_mappings or {})}

BUSINESS RULE (MANDATORY):
{_format_rule(business_rule)}

HARD GUARDRAILS:
{(guardrails or "None").strip()}

OPTIONAL SUPPORTING CONTEXT:
{_format_supporting_context(semantic_context)}

HARD RULES:
- NEVER invent column names.
- ONLY use exact columns from AVAILABLE COLUMNS.
- NEVER convert business concepts into fake columns.
- RESOLVED COLUMN MAPPINGS are mandatory and override your guesses.
- BUSINESS RULE is mandatory and must be applied exactly when present.
- If required fields are missing, return a one-row DataFrame with status = "CANNOT_COMPUTE" and a reason column.
- NEVER use SQL.
- ONLY use DataFrame API.
- ALWAYS assign the final result to final_df.

Before code, write only short # comments:
# 1. columns used
# 2. cleaning needed
# 3. transformation strategy

Return ONLY executable PySpark code.

USER QUESTION:
{question}
"""
