import posixpath

from config.settings import settings
from execution.databricks_executor import DatabricksExecutor
from logger.logger import get_logger

logger = get_logger(__name__)


WIKI_FILES = {
    "attrition": "support/attrition_logic.md",
    "compensation": "support/compensation_rules.md",
    "performance": "support/performance_logic.md",
    "promotion": "support/promotion_logic.md",
    "retention": "support/retention_risk_signals.md",
    "aggregation": "support/aggregation_rules.md",
    "synonyms": "support/synonym_mapping.md",
    "time_windows": "support/time_windows.md",
    "reasoning": "support/reasoning_examples.md",
    "confidence": "support/confidence_levels.md",
}


ROUTING_MAP = {
    "attrition": ["attrition", "exit", "resign", "turnover", "regrettable", "voluntary", "involuntary"],
    "compensation": ["salary", "pay", "ctc", "compensation", "hike", "bonus", "incentive", "payroll"],
    "performance": ["performance", "rating", "appraisal", "review", "high performer"],
    "promotion": ["promotion", "promote", "eligible", "ready", "career", "growth", "advance"],
    "retention": ["retention", "risk", "flight risk", "stay", "keep", "lose", "leaving"],
    "aggregation": ["average", "sum", "count", "total", "avg", "aggregate", "group by"],
    "synonyms": ["what column", "what field", "which column", "column name", "closest real column"],
    "time_windows": ["last year", "last month", "quarterly", "annually", "period", "time range", "recent"],
    "reasoning": ["hidden", "despite", "why is", "root cause", "explain"],
    "confidence": ["confidence", "how sure", "reliable", "approximate", "estimate"],
}


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
        ],
    },
    "compensation": {
        "question_terms": ["salary", "compensation", "payroll", "bonus", "pay zone", "salary hike", "hike"],
        "schema_terms": ["salary", "annualsalary", "bonus", "payzone", "salary_hike"],
    },
    "performance": {
        "question_terms": ["performance rating", "performance review", "appraisal", "high performer", "employee rating"],
        "schema_terms": ["performance", "rating", "appraisal", "current employee rating"],
    },
    "promotion": {
        "question_terms": ["promotion", "promoted", "promotion eligibility", "promotion ready", "promotion readiness"],
        "schema_terms": ["promotion", "eligible", "readiness"],
    },
    "retention": {
        "question_terms": ["retention risk", "flight risk", "risk of leaving", "retention"],
        "schema_terms": ["retention", "risk", "stay", "leaving"],
    },
    "time_windows": {
        "question_terms": ["last year", "last month", "this quarter", "annual", "monthly", "quarterly", "over time", "during"],
        "schema_terms": ["date", "month", "quarter", "year", "exitdate"],
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
    "confidence": 20
}


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

    return scores


def _get_wiki_root() -> str:
    wiki_root = settings.DOMAIN_WIKI_ROOTS.get("hr", "").strip()
    if not wiki_root:
        raise RuntimeError("HR wiki root is not configured")
    return wiki_root.replace("\\", "/").rstrip("/")


def load_wiki_file(key: str) -> str:
    relative_path = WIKI_FILES.get(key)
    if not relative_path:
        logger.warning("Unknown support wiki file key requested: %s", key)
        return ""

    wiki_root = _get_wiki_root()
    full_path = posixpath.join(wiki_root, relative_path)
    logger.info("Loading support wiki file | key=%s | path=%s", key, full_path)

    try:
        content = DatabricksExecutor().read_volume_text(full_path)
        logger.info("Support wiki file loaded | key=%s | chars=%d", key, len(content))
        return content
    except Exception as exc:
        logger.warning("Support wiki file unavailable | key=%s | path=%s | error=%s", key, full_path, str(exc))
        return ""


def load_domain_context_text(domain: str, filename: str) -> str:
    """
    Load a structured domain-context artifact.

    Runtime source of truth:
    1. Databricks Volume path only
    """
    candidates: list[str] = []
    wiki_root = settings.DOMAIN_WIKI_ROOTS.get(domain, "").strip()
    if wiki_root:
        normalized_root = wiki_root.replace("\\", "/").rstrip("/")
        candidates.append(posixpath.join(normalized_root, filename))

    for candidate in candidates:
        try:
            content = DatabricksExecutor().read_volume_text(candidate)
            logger.info(
                "Loaded domain context from Databricks Volume | domain=%s | file=%s | path=%s | chars=%d",
                domain,
                filename,
                candidate,
                len(content)
            )
            return content
        except Exception as exc:
            logger.warning(
                "Databricks domain context unavailable | domain=%s | file=%s | path=%s | error=%s",
                domain,
                filename,
                candidate,
                str(exc)
            )

    raise RuntimeError(
        f"Domain context file not found in Databricks Volume for domain={domain}, filename={filename}"
    )


def retrieve_relevant_chunks(question: str, schema_columns: str, top_k: int = 1) -> str:
    scores = _score_topic_routes(question, schema_columns)
    ranked_keys = [
        key
        for key, _ in sorted(
            scores.items(),
            key=lambda item: (-item[1], -ROUTE_PRIORITY.get(item[0], 0), item[0])
        )
        if scores[key] > 0
    ][:max(1, top_k)]

    selected_key = ranked_keys[0] if ranked_keys else None
    logger.info(
        "Support wiki retrieval selection | selected_key=%s | scores=%s",
        selected_key,
        {key: scores.get(key, 0) for key in ranked_keys[:1]}
    )

    if not selected_key:
        logger.info("Support wiki retrieval complete | selected=0 | loaded=0 | total_chars=0")
        return ""

    content = load_wiki_file(selected_key).strip()
    logger.info(
        "Support wiki retrieval complete | selected=1 | loaded=%d | total_chars=%d",
        1 if content else 0,
        len(content)
    )
    return content
