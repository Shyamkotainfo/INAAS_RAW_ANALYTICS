def _format_columns(columns: str) -> str:
    column_names = [column.strip() for column in columns.split(",") if column.strip()]
    return "\n".join(f"- {column}" for column in column_names) or "- None"


def _format_mappings(resolved_mappings: dict[str, str]) -> str:
    if not resolved_mappings:
        return "- None"
    return "\n".join(f"- {key} -> {value}" for key, value in resolved_mappings.items())


def _format_rule(business_rule: dict | None) -> str:
    if not business_rule:
        return "No exact business rule matched the question."

    lines = [
        f"Rule name: {business_rule.get('name', 'unknown_rule')}",
        f"Definition: {business_rule.get('definition', 'N/A')}",
    ]

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


def get_correction_prompt(
    columns: str,
    question: str,
    failing_code: str,
    error_message: str,
    resolved_mappings: dict[str, str] | None = None,
    business_rule: dict | None = None,
    guardrails: str | None = None,
    semantic_context: str | None = None
) -> str:
    return f"""
You are fixing executable PySpark code.

AVAILABLE COLUMNS:
{_format_columns(columns)}

RESOLVED COLUMN MAPPINGS (MANDATORY):
{_format_mappings(resolved_mappings or {})}

BUSINESS RULE (MANDATORY):
{_format_rule(business_rule)}

HARD GUARDRAILS:
{(guardrails or "None").strip()}

OPTIONAL SUPPORTING CONTEXT:
{(semantic_context or "None").strip()}

FAILING CODE:
{failing_code}

EXECUTION ERROR:
{error_message}

HARD RULES:
- NEVER invent column names.
- ONLY use exact columns from AVAILABLE COLUMNS.
- NEVER convert business concepts into fake columns.
- RESOLVED COLUMN MAPPINGS are mandatory and override your guesses.
- BUSINESS RULE is mandatory and must be preserved.
- If required fields are missing, return a one-row DataFrame with status = "CANNOT_COMPUTE" and a reason column.
- NEVER use SQL.
- ONLY use DataFrame API.
- ALWAYS assign the final result to final_df.

Before code, write only short # comments:
# 1. root cause
# 2. columns used
# 3. fix strategy

Return ONLY executable PySpark code.

USER QUESTION:
{question}
"""
