import posixpath

from config.settings import settings
from execution.databricks_executor import DatabricksExecutor
from logger.logger import get_logger

logger = get_logger(__name__)


WIKI_FILES = {
    "attrition": "wiki/core/attrition_logic.md",
    "compensation": "wiki/core/compensation_rules.md",
    "performance": "wiki/core/performance_logic.md",
    "promotion": "wiki/core/promotion_logic.md",
    "retention": "wiki/core/retention_risk_signals.md",
    "aggregation": "wiki/operational/aggregation_rules.md",
    "synonyms": "wiki/operational/synonym_mapping.md",
    "fallbacks": "wiki/operational/fallback_rules.md",
    "time_windows": "wiki/operational/time_windows.md",
    "hard_rules": "wiki/guardrails/hard_rules.md",
    "negatives": "wiki/guardrails/negative_definitions.md",
    "unsupported": "wiki/guardrails/unsupported_analysis.md",
    "reasoning": "wiki/output/reasoning_examples.md",
    "confidence": "wiki/output/confidence_levels.md",
}


ROUTING_MAP = {
    "attrition": ["attrition", "exit", "resign", "turnover", "churn", "left", "regrettable", "voluntary", "involuntary"],
    "compensation": ["salary", "pay", "ctc", "compensation", "hike", "bonus", "incentive", "payroll", "variable"],
    "performance": ["performance", "rating", "appraisal", "kra", "review", "high performer"],
    "promotion": ["promotion", "promote", "eligible", "ready", "career", "growth", "advance"],
    "retention": ["retention", "risk", "flight risk", "stay", "keep", "lose", "leaving"],
    "aggregation": ["average", "sum", "count", "total", "avg", "aggregate", "group by"],
    "synonyms": ["what column", "what field", "what does", "what is", "which column", "column name"],
    "fallbacks": ["missing", "unavailable", "no data", "cannot compute", "absent"],
    "time_windows": ["last year", "last month", "quarterly", "annually", "period", "time range", "recent"],
    "negatives": ["not mean", "does not mean", "misinterpret", "wrong assumption"],
    "unsupported": ["dissatisfaction", "burnout", "morale", "culture", "individual risk", "predict who"],
    "reasoning": ["hidden", "despite", "why is", "which department", "root cause", "explain"],
    "confidence": ["confidence", "how sure", "reliable", "approximate", "estimate"],
}


# These files are injected on every query regardless of scoring
ALWAYS_INCLUDE = ["hard_rules", "aggregation"]


TOPIC_INTENT_MAP = {
    "attrition": {
        "question_terms": [
            "attrition",
            "attrition rate",
            "employee exits",
            "exit rate",
            "turnover",
            "turnover rate",
            "regrettable attrition",
            "voluntary attrition",
            "involuntary attrition",
        ],
        "schema_terms": [
            "terminationtype",
            "terminationdescription",
            "employeestatus",
            "exitdate",
            "termination date",
            "termination type",
            "termination description",
        ],
    },
    "compensation": {
        "question_terms": [
            "salary",
            "compensation",
            "payroll",
            "bonus",
            "pay zone",
            "salary hike",
            "hike",
        ],
        "schema_terms": [
            "salary",
            "annualsalary",
            "bonus",
            "payzone",
            "salary_hike",
        ],
    },
    "performance": {
        "question_terms": [
            "performance rating",
            "performance review",
            "appraisal",
            "high performer",
            "employee rating",
        ],
        "schema_terms": [
            "performance",
            "rating",
            "appraisal",
            "current employee rating",
        ],
    },
    "promotion": {
        "question_terms": [
            "promotion",
            "promoted",
            "promotion eligibility",
            "promotion ready",
            "promotion readiness",
        ],
        "schema_terms": [
            "promotion",
            "eligible",
            "readiness",
        ],
    },
    "retention": {
        "question_terms": [
            "retention risk",
            "flight risk",
            "risk of leaving",
            "retention",
        ],
        "schema_terms": [
            "retention",
            "risk",
            "stay",
            "leaving",
        ],
    },
    "synonyms": {
        "question_terms": [
            "what column",
            "which column",
            "what field",
            "column name",
            "map to",
            "maps to",
            "closest real column",
            "by department",
        ],
        "schema_terms": [
            "departmenttype",
            "division",
            "businessunit",
            "terminationtype",
            "employeestatus",
            "salary",
            "bonus",
        ],
    },
    "time_windows": {
        "question_terms": [
            "last year",
            "last month",
            "this quarter",
            "annual",
            "monthly",
            "quarterly",
            "over time",
            "during",
        ],
        "schema_terms": [
            "date",
            "month",
            "quarter",
            "year",
            "exitdate",
        ],
    },
}


ROUTE_PRIORITY = {
    "attrition": 100,
    "compensation": 90,
    "promotion": 80,
    "retention": 70,
    "performance": 60,
    "time_windows": 50,
    "synonyms": 40,
    "reasoning": 30,
    "confidence": 20,
    "fallbacks": 10,
    "unsupported": 10,
    "negatives": 10,
}


COLUMN_PRIORITY_MAP = {
    "attrition": [
        "TerminationType",
        "TerminationDescription",
        "EmployeeStatus",
        "ExitDate",
    ],
    "department": [
        "DepartmentType",
        "Division",
        "BusinessUnit",
    ],
    "compensation": [
        "Salary",
        "Bonus",
        "PayZone",
    ],
}


STRICT_OVERRIDE_BLOCKS = {
    "synonyms": """# Derived Metric Mappings

## Attrition

Attrition is NOT a physical column.

Use:
- TerminationType
- TerminationDescription
- EmployeeStatus
- ExitDate

Do NOT assume:
- attrition
- attrition_flag
- employee_attrition

Attrition must be derived using exit indicators.

---

## Department

Department maps to:
- DepartmentType
- Division
- BusinessUnit

Do NOT assume:
- Department

Always use the closest real schema column.

---

## Compensation

Compensation maps to:
- Salary
- AnnualSalary
- Bonus
- PayZone
- Current Employee Rating (only when compensation-related scoring exists)

Do NOT assume:
- compensation
- ctc
""",
    "hard_rules": """# Derived Metrics Rule

Business concepts are often derived metrics, not physical columns.

Examples:
- attrition
- regrettable attrition
- retention risk
- promotion readiness
- compensation competitiveness

These should be built from available schema columns.

Never assume a direct physical column exists.

If the concept exists in business context but not in AVAILABLE COLUMNS:
1. derive it from real columns
2. or explicitly state insufficient data

Never invent:
- attrition
- department
- retention_score
- manager_quality_score
""",
    "attrition": """# Attrition Rate Formula

Attrition rate should be calculated as:

Exits during selected time period
/
Average active employee population during the same time period

Do NOT use:
all historical exits / all employees

Do NOT use:
TerminationType is not null alone without time filtering

Always apply:
time window rules from time_windows.md
""",
}


def _extract_schema_columns(schema_columns: str) -> list[str]:
    return [column.strip() for column in (schema_columns or "").split(",") if column.strip()]


def _phrase_hits(text: str, phrases: list[str]) -> int:
    return sum(1 for phrase in phrases if phrase in text)


def _score_topic_routes(question: str, schema_columns: str) -> dict[str, int]:
    lowered_question = (question or "").lower()
    lowered_columns = (schema_columns or "").lower()

    scores: dict[str, int] = {}
    for key, keywords in ROUTING_MAP.items():
        score = 0
        for keyword in keywords:
            if keyword in lowered_question:
                score += 2
            if keyword in lowered_columns:
                score += 1
        scores[key] = score

    for key, intent_config in TOPIC_INTENT_MAP.items():
        question_hits = _phrase_hits(lowered_question, intent_config.get("question_terms", []))
        schema_hits = _phrase_hits(lowered_columns, intent_config.get("schema_terms", []))
        scores[key] = scores.get(key, 0) + (question_hits * 12) + (schema_hits * 7)

    if "attrition" in lowered_question:
        scores["attrition"] = scores.get("attrition", 0) + 25
        scores["synonyms"] = scores.get("synonyms", 0) + 8
        scores["time_windows"] = scores.get("time_windows", 0) + 6

    if "department" in lowered_question:
        scores["synonyms"] = scores.get("synonyms", 0) + 10

    if "attrition rate" in lowered_question or "turnover rate" in lowered_question:
        scores["attrition"] = scores.get("attrition", 0) + 20
        scores["time_windows"] = scores.get("time_windows", 0) + 10

    if "by department" in lowered_question:
        scores["synonyms"] = scores.get("synonyms", 0) + 12

    if "salary" in lowered_question or "compensation" in lowered_question:
        scores["compensation"] = scores.get("compensation", 0) + 12
        scores["synonyms"] = scores.get("synonyms", 0) + 6

    return scores


def _build_preferred_mapping_block(selected_keys: list[str], schema_columns: str, question: str) -> str:
    available_columns = _extract_schema_columns(schema_columns)
    normalized_lookup = {column.lower(): column for column in available_columns}

    concepts = []
    if "attrition" in selected_keys:
        concepts.append("attrition")
    if "compensation" in selected_keys:
        concepts.append("compensation")

    lowered_question = (question or "").lower()
    lowered_columns = (schema_columns or "").lower()
    if "department" in lowered_question or "department" in lowered_columns:
        concepts.append("department")

    seen = set()
    ordered_concepts = []
    for concept in concepts:
        if concept not in seen:
            seen.add(concept)
            ordered_concepts.append(concept)

    mapping_lines = []
    for concept in ordered_concepts:
        preferred = []
        for column in COLUMN_PRIORITY_MAP.get(concept, []):
            matched = normalized_lookup.get(column.lower())
            if matched:
                preferred.append(matched)

        if preferred:
            mapping_lines.append(f"{concept.title()} -> {', '.join(preferred)}")

    if not mapping_lines:
        return ""

    return "# Preferred Column Mapping\n\n" + "\n".join(mapping_lines)


def _build_chunk_for_key(key: str, schema_columns: str, question: str) -> str:
    parts = []

    if key in STRICT_OVERRIDE_BLOCKS:
        parts.append(STRICT_OVERRIDE_BLOCKS[key])

    wiki_content = load_wiki_file(key)
    if wiki_content.strip():
        parts.append(wiki_content)

    return "\n\n---\n\n".join(part for part in parts if part.strip())


def _get_wiki_root() -> str:
    wiki_root = settings.DOMAIN_WIKI_ROOTS.get("hr", "").strip()
    if not wiki_root:
        raise RuntimeError("HR wiki root is not configured")

    return wiki_root.replace("\\", "/").rstrip("/")


def load_wiki_file(key: str) -> str:
    relative_path = WIKI_FILES.get(key)
    if not relative_path:
        logger.warning("Unknown wiki file key requested: %s", key)
        return ""

    wiki_root = _get_wiki_root()
    full_path = posixpath.join(wiki_root, relative_path)
    logger.info("Loading wiki file | key=%s | path=%s", key, full_path)

    try:
        content = DatabricksExecutor().read_volume_text(full_path)
        logger.info("Wiki file loaded | key=%s | chars=%d", key, len(content))
        return content
    except Exception as exc:
        logger.warning("Wiki file unavailable | key=%s | path=%s | error=%s", key, full_path, str(exc))
        return ""


def retrieve_relevant_chunks(question: str, schema_columns: str, top_k: int = 3) -> str:
    scores = _score_topic_routes(question, schema_columns)

    ranked_keys = [
        key
        for key, _ in sorted(
            scores.items(),
            key=lambda item: (-item[1], -ROUTE_PRIORITY.get(item[0], 0), item[0])
        )
        if key not in ALWAYS_INCLUDE and scores[key] > 0
    ][:top_k]

    selected_keys = []
    for key in ALWAYS_INCLUDE + ranked_keys:
        if key not in selected_keys:
            selected_keys.append(key)

    preferred_mapping = _build_preferred_mapping_block(selected_keys, schema_columns, question)

    logger.info(
        "Wiki retrieval selection | top_k=%d | selected_keys=%s | scores=%s",
        top_k,
        selected_keys,
        {key: scores.get(key, 0) for key in selected_keys}
    )

    loaded_chunks = []
    if preferred_mapping:
        loaded_chunks.append(preferred_mapping)

    loaded_chunks.extend(
        _build_chunk_for_key(key, schema_columns, question) for key in selected_keys
    )
    non_empty_chunks = [chunk for chunk in loaded_chunks if chunk.strip()]

    logger.info(
        "Wiki retrieval complete | selected=%d | loaded=%d | total_chars=%d",
        len(selected_keys),
        len(non_empty_chunks),
        sum(len(chunk) for chunk in non_empty_chunks)
    )

    return "\n\n---\n\n".join(non_empty_chunks)
