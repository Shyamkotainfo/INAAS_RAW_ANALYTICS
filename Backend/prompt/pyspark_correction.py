def _format_columns(columns: str) -> str:
    column_names = [column.strip() for column in columns.split(",") if column.strip()]
    return "\n".join(f"- {column}" for column in column_names) or "- None"


def _format_mappings(resolved_mappings: dict[str, str]) -> str:
    if not resolved_mappings:
        return "- None"
    return "\n".join(f"- {key} -> {value}" for key, value in resolved_mappings.items())


def _format_rule(business_rule: dict | None) -> str:
    if not business_rule:
        return "None. Use schema-only reasoning and do not invent business logic."

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
    semantic_context: str | None = None,
    resolved_terms: dict[str, str] | None = None,
    resolved_mappings: dict[str, str] | None = None,
    business_rule: dict | None = None,
    guardrails: str | None = None
) -> str:
    return f"""
You are fixing executable PySpark code.

AVAILABLE COLUMNS:
{_format_columns(columns)}

RESOLVED COLUMN MAPPINGS (USE WHEN PRESENT):
{_format_mappings(resolved_mappings or {})}

BUSINESS RULE (USE WHEN PRESENT):
{_format_rule(business_rule)}

HARD GUARDRAILS:
{(guardrails or "None").strip()}

EXECUTION ERROR:
{error_message}

HARD RULES:
- NEVER invent column names.
- ONLY use exact columns from AVAILABLE COLUMNS.
- NEVER convert business concepts into fake columns.
- RESOLVED COLUMN MAPPINGS override your guesses when present.
- BUSINESS RULE must be preserved when present.
- If required fields are missing, return a one-row DataFrame with status = "CANNOT_COMPUTE" and a reason column.
- NEVER use SQL.
- ONLY use DataFrame API.
- When comparing, sorting, or aggregating string-encoded numeric/date/flag columns, use the helper functions first.
- NEVER redefine helper functions such as as_date, as_double, as_int, as_bool_flag, or as_priority_rank.
- If a priority column contains labels like High, Medium, Low, rank it with as_priority_rank instead of casting it to int.
- ALWAYS assign the final result to final_df.

1. Read the error message carefully. Identify the ROOT CAUSE:
   - AnalysisException -> wrong column name, incompatible schema, wrong join key, bad cast
   - AttributeError -> method called on wrong object type (e.g. F.groupBy instead of df.groupBy)
   - ParseException -> syntax error in string expressions
   - IllegalArgumentException -> bad function arguments (e.g. wrong format string)
   - TypeError -> Python-level type mismatch
   - AnalysisException        → wrong column name, incompatible schema, wrong join key, bad cast
   - [NOT_COLUMN] error       → Argument `condition` should be a Column, got bool. 
     (FIX: Wrap python variables or scalars in F.lit() inside F.when)
   - AttributeError           → method called on wrong object type (e.g. F.groupBy instead of df.groupBy)
   - ParseException            → syntax error in string expressions
   - IllegalArgumentException → bad function arguments (e.g. wrong format string)
   - TypeError                → Python-level type mismatch
Before code, write only short # comments:
# 1. root cause
# 2. columns used
# 3. fix strategy

Return ONLY executable PySpark code.

3. ABSOLUTE RULES:
   - NEVER invent base dataset column names. Use ONLY the exact names in AVAILABLE COLUMNS for the raw input schema.
   - Derived columns created via `.alias(...)`, `.withColumn(...)`, aggregate aliases, or joins are valid AFTER they are introduced.
   - Semantic-layer concepts are not columns unless the exact column exists in AVAILABLE COLUMNS.
   - NEVER use spark.sql(). Only DataFrame APIs.
   - NEVER use python loops or list comprehensions.
   - groupBy is a DataFrame method. Use df.groupBy(...). NEVER F.groupBy(...).
   - DO NOT use df.count(). Use df.select(F.count("*").alias("total_rows")).
   - SCALAR COMPARISONS: In F.when, comparisons between python variables MUST use F.lit().
   - NEVER use F.col() for columns not in the source df or not yet created by a transformation.
   - For unionByName, all intermediate DataFrames must have identical column structures.
   - Avoid crossJoin.

4. If the error is due to a missing base dataset column:
   - Remove that column reference entirely.
   - Substitute the closest semantically matching column from AVAILABLE COLUMNS, or omit the step.
   - Do NOT keep semantic-layer-only concepts such as Department if they are not present in AVAILABLE COLUMNS.

5. If the error is an AnalysisException about schema mismatch before unionByName:
   - Align ALL intermediate DataFrames to have exactly the same column names and types.

6. Compensation-specific fixes:
   - Treat `Pay` as text unless code explicitly normalizes it into a numeric derived column first.
   - Before compensation aggregation, ranking, filtering, min/max, variance, or percentile logic, normalize `Pay`.
   - Exclude null or unparseable normalized pay values from compensation math.
   - Do NOT treat "compensation band" as average compensation unless SEMANTIC CONTEXT explicitly defines a band model.
   - If SEMANTIC CONTEXT does not define compensation bands, prefer designation-level average compensation as the fallback comparison.
6. When correcting failed code:
   - Do not only fix missing columns.
   - Re-evaluate whether the business metric logic itself is correct.
   - Especially for attrition, compensation, promotion, and retention analysis.
   - If the first attempt used an invalid business assumption, rewrite the logic instead of only replacing column names.

USER QUESTION:
{question}
"""
