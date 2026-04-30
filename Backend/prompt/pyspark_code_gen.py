from prompt.wiki_retriever import retrieve_relevant_chunks


def get_pyspark_prompt(
    columns: str,
    question: str,
    semantic_context: str | None = None,
    resolved_terms: dict[str, str] | None = None
) -> str:
    pass


# def build_semantic_context(question: str, columns: str, top_k: int = 3) -> str:
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

MODE B — ANALYTICAL (everything else):
  Aggregations, trends, filters, top-N, distributions, comparisons, cohorts, anomalies.

=====================================================
AVAILABLE COLUMNS
=====================================================

{columns}

=====================================================
SEMANTIC CONTEXT
=====================================================

{semantic_context or "No additional semantic context provided."}

SEMANTIC CONTEXT USAGE RULES:
- Treat SEMANTIC CONTEXT as business guidance only, not as additional schema.
- NEVER use a semantic concept as a DataFrame column unless that exact column exists in AVAILABLE COLUMNS.
- If SEMANTIC CONTEXT mentions concepts like Department, Manager, Location, Business Unit, band, or level but no matching base column exists, do not generate PySpark that references them.
- If the user asks for an unavailable semantic concept, answer with the closest supported available columns instead of inventing a field.

=====================================================
RESOLVED TERM MAPPINGS
=====================================================

{resolved_terms or "No explicit term mappings resolved."}

RESOLUTION RULES:
- If the user uses a business term that appears in RESOLVED TERM MAPPINGS, use the mapped real column from AVAILABLE COLUMNS.
- Prefer resolved mappings over guessing.
- Only fail a concept when no safe mapping exists and no matching base column exists.
BUSINESS CONTEXT
=====================================================

{semantic_context or "No additional business context provided."}

BUSINESS CONTEXT RULES:
- Treat BUSINESS CONTEXT as business guidance only, not as physical schema.
- Use BUSINESS CONTEXT to improve business reasoning, KPI interpretation, and guardrail awareness.
- NEVER invent columns from BUSINESS CONTEXT alone.
- ALWAYS prioritize the real dataset columns listed in AVAILABLE COLUMNS.
- If BUSINESS CONTEXT mentions a concept but no matching column exists in AVAILABLE COLUMNS, do not generate PySpark that references a made-up field.
- Use BUSINESS CONTEXT to improve reasoning, not to extend the schema.

=====================================================
MODE A — META / STRUCTURAL INTELLIGENCE
=====================================================

When the question is a META question, classify it into one of these 8 STRUCTURAL TYPES
and apply the corresponding PySpark strategy.

Use ONLY columns from the AVAILABLE COLUMNS list above.
Reason about each column's role using its name, naming pattern, and position in the schema.

--- COLUMN CLASSIFICATION RULES (apply before any META type) ---

IDENTIFIER columns (skip for groupBy, skip for measures):
  - Column name ends with: _id, _key, _uuid, _ref, _no, _num, _number, _code (unless it's a category code like status_code or region_code)
  - Column name is exactly: id, uuid, key
  - Very high cardinality implied by naming (invoice number, transaction id, order number)

FREE-TEXT columns (skip for groupBy):
  - Column name contains: description, comment, remark, note, address, feedback, summary, text, narrative

PERSON IDENTIFIER columns (skip for groupBy):
  - Column name contains: first_name, last_name, full_name, email, phone, mobile, url, website, linkedin

DIMENSION columns (good for groupBy — low-to-medium cardinality categorical or date):
  - String columns that represent: status, category, type, region, country, city, state,
    department, segment, channel, gender, designation, qualification, employment_type,
    industry, source, medium, priority, flag, tier, grade, band, level
  - Date or timestamp columns

MEASURE columns (good for aggregation — numeric):
  - Column name contains: amount, revenue, salary, price, cost, score, count, qty,
    quantity, rate, fee, tax, discount, profit, spend, budget, balance, age, tenure,
    duration, days, hours, units, weight, volume

REPEATED / HISTORY columns (skip unless explicitly asked):
  - Column name follows a pattern like: company1/company2/company3, jobtitle1/jobtitle2,
    skill1/skill2/skill3, prev_employer_1/prev_employer_2

--- TYPE 1: GRAIN DETECTION ---
Trigger: "what does one row represent", "what is the grain", "unit of analysis", "what is each record"
Strategy:
  - Identify IDENTIFIER columns from AVAILABLE COLUMNS using naming rules above.
  - Use approxCountDistinct on identifier-like columns vs total row count.
  - If approxCountDistinct ≈ total rows → primary key / grain column.
  - IMPORTANT: When comparing collected scalars (like row count) in F.when, wrap them in F.lit().
  - Output schema: column_name | distinct_count | total_rows | is_grain_candidate

--- TYPE 2: DIMENSION DETECTION ---
Trigger: "what are the dimensions", "what can I slice by", "categorical columns", "grouping columns", "only dimensions columns"
Strategy:
  - Select ONLY DIMENSION columns from AVAILABLE COLUMNS using classification rules above.
  - Skip IDENTIFIER, FREE-TEXT, PERSON IDENTIFIER, REPEATED/HISTORY, and MEASURE columns.
  - Do NOT perform data-level aggregations (like groupBy) to list values unless explicitly asked.
  - Output a metadata DataFrame listing the dimension columns.
  - Create static rows by selecting F.lit() from df.limit(1) and combining with unionByName.
  - Output schema: column_type (always 'DIMENSION') | column_name
  - Example: d1 = df.limit(1).select(F.lit("DIMENSION").alias("column_type"), F.lit("Gender").alias("column_name"))

--- TYPE 3: MEASURE DETECTION ---
Trigger: "what are the measures", "what can I aggregate", "numeric columns", "what are the KPIs", "metric columns"
Strategy:
  - Select ONLY MEASURE columns from AVAILABLE COLUMNS using classification rules above.
  - Exclude numeric columns that behave like categories (year, month, flag, code, rating band, bucket).
  - Do NOT compute global distributions unless explicitly asked.
  - Output a metadata DataFrame listing the measure columns.
  - Create static rows by selecting F.lit() from df.limit(1) and combining with unionByName.
  - Output schema: column_type (always 'MEASURE') | column_name

--- TYPE 4: TIME DIMENSION DETECTION ---
Trigger: "what dates", "time period", "date range", "how far back", "when does this data start"
Strategy:
  - Identify all date/timestamp columns from AVAILABLE COLUMNS by name pattern:
    contains date, time, created, updated, modified, joined, timestamp, period, month, year, day.
  - For each time column: compute min, max, approxCountDistinct.
  - Cast string date columns using F.to_date() before computing min/max.
  - Output schema: time_column | min_date | max_date | approx_distinct_values

--- TYPE 5: DATA QUALITY PROFILING ---
Trigger: "how clean is the data", "nulls", "missing values", "duplicates", "data quality", "completeness"
Strategy:
  - For EVERY column in AVAILABLE COLUMNS: compute null_count using F.sum(F.when(F.col(...).isNull(), 1).otherwise(0)).
  - Derive null_pct = null_count / total_rows * 100.
  - Flag: null_pct > 20 → LOW_QUALITY, 5–20 → MEDIUM_QUALITY, else HIGH_QUALITY.
  - For duplicate detection: groupBy ALL columns → filter count > 1.
  - Build two separate DataFrames: null profile and duplicate count. Assign duplicate result to final_df.
  - Output schema (null profile): column_name | null_count | null_pct | quality_flag

--- TYPE 6: DATASET DOMAIN / TYPE DETECTION ---
Trigger: "what kind of dataset", "what business process", "what domain", "what is this data about"
Strategy:
  - Reason from AVAILABLE COLUMNS names to infer domain using keyword matching:
      Transactional  → order, invoice, payment, revenue, product, customer, cart
      HR             → employee, salary, department, designation, leave, attendance, headcount
      Marketing      → campaign, clicks, impressions, channel, conversion, spend, lead
      Financial      → ledger, debit, credit, account, journal, balance, gl, fiscal
      Support        → ticket, priority, sla, resolution, agent, escalation, case
      Healthcare     → diagnosis, claim, provider, patient, procedure, facility, icd
      IoT/Sensor     → device, sensor, reading, alert, temperature, voltage, telemetry
      Inventory      → warehouse, stock, shipment, supplier, sku, reorder, bin
      Survey         → respondent, score, rating, response, question, feedback, nps
  - Output: a single-row DataFrame with columns: inferred_domain | confidence_reason | probable_grain | suggested_dimensions | suggested_measures
  - Use F.lit() to build this output as a static summary DataFrame.

--- TYPE 7: RELATIONSHIP / JOIN KEY DETECTION ---
Trigger: "what can this join with", "foreign keys", "key columns", "what relates to what"
Strategy:
  - Identify columns ending with: _id, _key, _code, _ref, _num, _no, _uuid from AVAILABLE COLUMNS.
  - Classify each as: PRIMARY KEY (likely unique per row) or FOREIGN KEY (reused across rows).
  - Use approxCountDistinct vs total rows to distinguish.
  - Output schema: column_name | key_type | probable_referenced_entity | distinct_count

--- TYPE 8: FULL SCHEMA OVERVIEW ---
Trigger: "give me an overview", "profile this dataset", "summarize the schema",
         "what is in this dataset", "what columns do I have"
Strategy:
  - Classify EVERY column from AVAILABLE COLUMNS into one of:
    DIMENSION | MEASURE | IDENTIFIER | TIME | FREE_TEXT | REPEATED_HISTORY
  - Use the COLUMN CLASSIFICATION RULES above to assign each column a role.
  - For each column also compute: null_count, approxCountDistinct.
  - Output schema: column_name | classification | data_role_reason | null_count | distinct_count

--- TYPE 9: HIERARCHY DETECTION ---
Trigger: "what hierarchies exist", "country state city", "parent child", "is there a hierarchy", "drill down"
Strategy:
  - Scan AVAILABLE COLUMNS for columns that form geographic or organizational cascades:
      Geographic: continent, country, region, state, city, district, zip, postal_code
      Organizational: company, division, department, team, sub_team
      Product: category, sub_category, product_group, product_name, sku
      Time: year, quarter, month, week, day
  - For each detected hierarchy, compute the count of distinct values at EACH level using approxCountDistinct.
  - Arrange levels from lowest cardinality (highest level) to highest cardinality (lowest level).
  - Output a single DataFrame describing the hierarchy levels:
  - Output schema: hierarchy_name | level_order | column_name | approx_distinct_values
  - Use F.lit() to build static column labels. Combine levels with unionByName.

--- TYPE 10: AGGREGATION LEVEL DETECTION ---
Trigger: "is this raw", "level of aggregation", "granularity", "is this summarized", "rolled up or transactional"
Strategy:
  - Identify the most likely grain/key column using approxCountDistinct vs row count (as in TYPE 1).
  - If the best unique-ratio column approaches 1.0 → likely RAW / transactional.
  - If no column approaches uniqueness and multiple numeric columns exist → likely PRE-AGGREGATED / rolled up.
  - Also check for presence of columns like total_, sum_, avg_, count_ as hints of aggregation.
  - Output: a single-row DataFrame with:
    inferred_level (RAW_TRANSACTIONAL | PRE_AGGREGATED | UNKNOWN) | confidence_reason | grain_column | row_count
  - Use F.lit() and df.select(F.count("*")) to build this output.

--- TYPE 11: TIME GAP / COVERAGE ANALYSIS ---
Trigger: "gaps in time", "missing days", "missing weeks", "are there gaps", "time coverage", "continuity"
Strategy:
  - Identify all date or timestamp columns in AVAILABLE COLUMNS by name pattern:
      contains: date, time, created, updated, joined, modified, period, month, year, day, timestamp.
  - For the primary time column (lowest in the name pattern priority list):
      a. Cast to date using F.to_date() if stored as string.
      b. Compute min_date, max_date, and approxCountDistinct of the date column.
      c. Compute expected_days = datediff(max_date, min_date) + 1.
      d. actual_distinct_days = approxCountDistinct result.
      e. gap_count = expected_days - actual_distinct_days.
  - Output schema: time_column | min_date | max_date | expected_days | actual_distinct_days | gap_count | has_gaps
  - has_gaps = gap_count > 0.
  - Use F.datediff, F.min, F.max, F.approx_count_distinct, F.lit, F.when for this.

--- TYPE 12: KPI DERIVATION SUGGESTIONS ---
Trigger: "what kpis", "what metrics can i derive", "business metrics", "primary business outcome", "what can i measure"
Strategy:
  - From AVAILABLE COLUMNS, identify MEASURE columns using classification rules (TYPE 3 rules).
  - For each MEASURE column: compute min, max, avg, sum (if numeric), stddev.
  - Also identify good DIMENSION columns for slicing.
  - Suggest derived KPIs by combining measures with standard aggregation patterns:
      total_ → F.sum | average_ → F.avg | rate_ → F.count with filter / total count
  - Output: for each measure, show its min/max/avg/sum and flag it as a candidate KPI.
  - Output schema: kpi_candidate | source_column | min_val | max_val | avg_val | sum_val | suggested_aggregation
  - Use F.lit() to add the suggested_aggregation label (e.g. "SUM", "AVG", "COUNT", "RATIO").

=====================================================
MODE B — ANALYTICAL INTELLIGENCE
=====================================================

1. DIMENSIONS VS MEASURES:
   - Dimensions (Categorical/Text/Time): used for groupBy, slicing, and dicing.
   - Measures (Numeric): used for aggregations — sum, avg, min, max, count, stddev.

2. DIMENSION SELECTION RULES:
   - Use COLUMN CLASSIFICATION RULES above to confirm a column is a DIMENSION before groupBy.
   - Do NOT use IDENTIFIER, FREE-TEXT, PERSON IDENTIFIER, or REPEATED/HISTORY columns as dimensions.
   - For dimension distribution output use schema: dimension_name | dimension_value | record_count.
   - For broad dimension-distribution questions, pick TOP 5 most useful dimensions only.
   - Use short variable names: d1, d2, d3, d4, d5.

3. MEASURE SELECTION RULES:
   - Numeric columns are measures by default.
   - If a numeric column has distinct_count < 20 semantically (year, flag, code, band) → treat as dimension.
   - Clean text-encoded measures: F.regexp_replace(F.col("col"), "[^0-9.-]", "").cast("double")

4. DATASET TYPE DEFAULTS:
   - Transactional: time, status, category, region, customer, product → dimensions; amount, count → measures.
   - Event/Log: event_time, event_type, source, device, session → dimensions; event_count → measure.
   - HR: department, location, designation, gender, join_date → dimensions; salary, bonus, age → measures.
   - Financial: period, cost_center, account, ledger, currency → dimensions; debit, credit, balance → measures.
   - Inventory: warehouse, supplier, category, shipment_date, stock_status → dimensions; quantity, cost → measures.
   - Marketing: campaign, channel, source, segment, date → dimensions; impressions, clicks, spend, revenue → measures.
   - Support: status, priority, category, channel, team → dimensions; resolution_time, ticket_count → measures.
   - Healthcare: provider, diagnosis, procedure, facility, payer, service_date → dimensions; claim_amount → measures.
   - Survey: segment, location, department, period → dimensions; score, rating, response_count → measures.
   - IoT: device, sensor_type, site, alert_status → dimensions; reading, temperature, voltage → measures.

5. ADVANCED ANALYTICAL OPERATIONS:
   - "Trend" / "Over time": groupBy date dimension → sort chronologically → rolling avg or growth rate.
   - "Distribution" / "Spread": F.percentile_approx, min/max/avg, or bucket ranges.
   - "Top N" / "Rank": Window.partitionBy().orderBy() with F.row_number() or F.rank().
   - "Cohort": group by first interaction date/attribute → track metrics over time.
   - "Anomaly" / "Outliers": records > 2 standard deviations from mean using F.stddev.

=====================================================
MESSY DATA HANDLING (APPLIES TO BOTH MODES)
=====================================================

Raw data is always dirty. ALWAYS anticipate nulls, mixed cases, and string-encoded numbers.
- String standardization : F.upper(F.trim(F.col("col")))
- Numeric text cleaning  : F.regexp_replace(F.col("col"), "[^0-9.-]", "").cast("double")
- Date string parsing    : F.to_date(F.col("col"), "yyyy-MM-dd") or F.to_timestamp(...)
- Null handling          : F.col("col").isNotNull() before aggregating when needed.
- If a column concept is mentioned but not found exactly, use RESOLVED TERM MAPPINGS when available.
- Do not infer a new schema column from SEMANTIC CONTEXT alone.

=====================================================
DERIVED COLUMN AND JOIN SAFETY
=====================================================

- AVAILABLE COLUMNS applies to the base dataset only.
- You MAY introduce derived columns through `.alias(...)`, `.withColumn(...)`, `.agg(...alias(...))`, and joins on intermediate DataFrames.
- Once a derived column is created, you MAY reference it in later steps on that DataFrame lineage.
- For joined DataFrames, you MAY use columns coming from either joined side, then create additional derived columns from the join result.
- Do not treat valid intermediate columns such as `avg_pay` as schema violations after they are created.

=====================================================
COMPENSATION ANALYSIS RULES
=====================================================

- Treat `Pay` as compensation-like text, not as a numeric column until you normalize it.
- Before any compensation filter, comparison, ranking, percentile, min/max, or aggregation:
  use `F.regexp_extract(F.upper(F.trim(F.col("Pay"))), "([0-9]+(?:\\.[0-9]+)?)", 1).cast("double")`
  and store it in a derived numeric column such as `pay_numeric`.
- If the cleaned compensation value is null, exclude it from compensation math.
- Do NOT interpret the phrase "compensation band" as "average compensation" unless explicit band metadata exists in SEMANTIC CONTEXT.
- If band metadata is not present in SEMANTIC CONTEXT and the user asks about compensation band:
  default to comparing against designation-level average compensation and make that fallback explicit.
- Do NOT invent salary bands, buckets, min/max thresholds, or percentile cutoffs unless they are explicitly defined in SEMANTIC CONTEXT or explicitly requested by the user.
- If the user asks for percentile-style or min/max banding and SEMANTIC CONTEXT does not define bands, compute those only when the user explicitly asks for that comparison.
- If BUSINESS CONTEXT suggests a business concept but the actual column does not exist, fall back only to the closest real column when that mapping is obvious from AVAILABLE COLUMNS.

=====================================================
ABSOLUTE RULES (NO EXCEPTIONS)
=====================================================

- NEVER INVENT COLUMN NAMES. ONLY use exact column names from AVAILABLE COLUMNS above.
- NEVER convert semantic-layer concepts into DataFrame columns unless the exact column exists in AVAILABLE COLUMNS.
- NEVER convert a business concept from BUSINESS CONTEXT into a DataFrame column unless that exact column exists in AVAILABLE COLUMNS.
- NEVER use SQL queries. NEVER spark.sql(). Only DataFrame APIs.
- NEVER use python loops (for, while), list comprehensions, or pandas logic.
- ALWAYS use F. prefix for all functions (e.g. F.col(), F.sum(), F.count()).
- DataFrames are immutable. Reassign or chain transformations.
- groupBy is a DataFrame method ONLY. Use df.groupBy(...). NEVER F.groupBy(...).
- DO NOT use df.count(). Use df.select(F.count("*").alias("total_rows")).
- Multi-condition filters: df.filter((condition1) & (condition2))
- SCALAR COMPARISONS: In F.when, comparisons between python variables MUST use F.lit().
  - BAD  : F.when(distinct_count == total_rows, "Yes")  <-- This fails.
  - GOOD : F.when(F.lit(distinct_count) == F.lit(total_rows), "Yes")
- NEVER use F.col() for columns not in the source df or not yet created by a transformation.
- Avoid crossJoin.
- Drop duplicates only when explicitly requested: df.dropDuplicates().
- For multi-dimension outputs, all intermediate DataFrames MUST have identical column structure before unionByName.
- FINAL result MUST always be assigned to exactly one variable named: final_df

=====================================================
CHAIN OF THOUGHT REASONING (MANDATORY)
=====================================================

Before writing ANY code, you MUST plan using # comments:
# 1. Mode    : Is this META (Mode A) or ANALYTICAL (Mode B)?
# 2. Type    : If Mode A, which structural type (1–8)? If Mode B, what analytical operation?
# 3. Columns : Which exact columns from AVAILABLE COLUMNS are relevant? Apply classification rules.
# 4. Cleaning: Note any string standardization, null handling, or regex cleaning needed.
# 5. Strategy: Outline the PySpark transformation chain (e.g. filter → groupBy → agg → orderBy).

=====================================================
OUTPUT FORMAT
=====================================================

Return ONLY executable PySpark code.
Start with your # chain-of-thought comments.
Then write the PySpark code.
Do NOT include conversational text outside of # comments.
Do NOT include markdown block markers (e.g. ```python).
The final line MUST assign the result to: final_df

=====================================================
EXAMPLES
=====================================================

CRITICAL: Columns used in examples below are HYPOTHETICAL.
DO NOT use them unless they EXACTLY match a column name in AVAILABLE COLUMNS above.

Example 1 — Dimension Detection (Mode A, Type 2):
Question: What are the probable dimensions of this dataset?

# 1. Mode    : META — Mode A
# 2. Type    : TYPE 2 — Dimension Detection
# 3. Columns : From AVAILABLE COLUMNS, identify top 5 DIMENSION columns using classification rules.
#              Skip IDENTIFIER, FREE-TEXT, PERSON IDENTIFIER, REPEATED/HISTORY columns.
#              Selected (hypothetical): Country, Gender, Status, Department, Category
# 4. Cleaning: Apply F.upper(F.trim(...)) on string columns to normalize values.
# 5. Strategy: groupBy each dimension → count → withColumn dimension_name → unionByName all 5.

d1 = df.groupBy(F.upper(F.trim(F.col("Country"))).alias("dimension_value")).agg(
    F.count("*").alias("record_count")
).withColumn("dimension_name", F.lit("Country")).select("dimension_name", "dimension_value", "record_count")

d2 = df.groupBy(F.upper(F.trim(F.col("Gender"))).alias("dimension_value")).agg(
    F.count("*").alias("record_count")
).withColumn("dimension_name", F.lit("Gender")).select("dimension_name", "dimension_value", "record_count")

d3 = df.groupBy(F.upper(F.trim(F.col("Status"))).alias("dimension_value")).agg(
    F.count("*").alias("record_count")
).withColumn("dimension_name", F.lit("Status")).select("dimension_name", "dimension_value", "record_count")

d4 = df.groupBy(F.upper(F.trim(F.col("Department"))).alias("dimension_value")).agg(
    F.count("*").alias("record_count")
).withColumn("dimension_name", F.lit("Department")).select("dimension_name", "dimension_value", "record_count")

d5 = df.groupBy(F.upper(F.trim(F.col("Category"))).alias("dimension_value")).agg(
    F.count("*").alias("record_count")
).withColumn("dimension_name", F.lit("Category")).select("dimension_name", "dimension_value", "record_count")

final_df = d1.unionByName(d2).unionByName(d3).unionByName(d4).unionByName(d5)

---

Example 2 — Data Quality (Mode A, Type 5):
Question: Are there nulls in this dataset?

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

---

Example 3 — Analytical Aggregation (Mode B):
Question: What is the average revenue for completed orders?

# 1. Mode    : ANALYTICAL — Mode B
# 2. Type    : Filtered aggregation
# 3. Columns : Status (DIMENSION — categorical), Revenue (MEASURE — numeric)
# 4. Cleaning: Trim/upper Status for comparison. Regex-clean Revenue before avg.
# 5. Strategy: filter Status == COMPLETED → avg on cleaned Revenue.

final_df = df.filter(
    F.col("Status").isNotNull() & (F.upper(F.trim(F.col("Status"))) == "COMPLETED")
).select(
    F.avg(F.regexp_replace(F.col("Revenue"), "[^0-9.-]", "").cast("double")).alias("avg_completed_revenue")
)

---

Example 4 — Time Series Trend (Mode B):
Question: Show me the daily sales count trend.

# 1. Mode    : ANALYTICAL — Mode B
# 2. Type    : Time Series Trend
# 3. Columns : Sale_Date (TIME), count of rows as measure
# 4. Cleaning: Cast Sale_Date to date if stored as string.
# 5. Strategy: groupBy Sale_Date → count → orderBy date ascending.

final_df = df.groupBy(F.to_date(F.col("Sale_Date"), "yyyy-MM-dd").alias("sale_date")).agg(
    F.count("*").alias("daily_sales")
).orderBy("sale_date")

=====================================================
USER QUESTION
=====================================================
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
