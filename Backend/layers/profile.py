# Backend/layers/profile.py

from logger.logger import get_logger
from llm.llm_query import invoke_llm
from layers.common import UNSTRUCTURED, load_profile_from_s3

logger = get_logger(__name__)

def handle_profile(question: str, dataset_id: str, format_bucket: str, active_file: dict = None) -> dict:
    """Answer distribution/quality questions from pre-computed stats."""
    if format_bucket == UNSTRUCTURED:
        return {
            "route": "PROFILE",
            "format_bucket": format_bucket,
            "answer": (
                "Statistical profiling is not meaningful for unstructured documents. "
                "Try asking a metadata or content question instead."
            ),
            "pyspark": None, "results": None,
        }

    profile = load_profile_from_s3(dataset_id) or (active_file.get("profiling", {}) if active_file else {})
    if not profile:
        return None  # Signal fallback to ADHOC

    total_rows = profile.get("row_count", "?")
    total_cols = profile.get("column_count", "?")
    cols       = profile.get("columns", [])

    col_stats = []
    for col in cols:
        name       = col.get("column_name", col.get("name", "?"))
        dtype      = col.get("data_type", "?")
        null_pct   = col.get("null_percentage", col.get("null_pct", 0))
        distinct   = col.get("distinct_count", "?")
        min_v      = col.get("min", None)
        max_v      = col.get("max", None)
        mean_v     = col.get("mean", None)
        top_vals   = col.get("top_values", col.get("sample_values", []))

        parts = [f"  • {name} [{dtype}] | nulls={null_pct}% | distinct={distinct}"]
        if min_v is not None and max_v is not None:
            parts.append(f"    range: {min_v} → {max_v}")
        if mean_v is not None:
            parts.append(f"    mean:  {mean_v}")
        if top_vals:
            if isinstance(top_vals[0], dict):
                vals_str = ", ".join(f"{v.get('value', '?')} ({v.get('count', '?')})" for v in top_vals[:5])
            else:
                vals_str = ", ".join(str(v) for v in top_vals[:5] if v is not None)
            parts.append(f"    top values: {vals_str}")
        col_stats.append("\n".join(parts))

    stats_text = "\n".join(col_stats)
    summary_text = f"Total rows: {total_rows} | Total columns: {total_cols}\n\n{stats_text}"

    try:
        llm_prompt = (
            f"The user asked: \"{question}\"\n\n"
            f"Here are the pre-computed column statistics:\n{summary_text}\n\n"
            "Answer the user's specific question using only the statistics above. "
            "Be direct and precise. Include numbers."
        )
        answer = invoke_llm(prompt=llm_prompt, temperature=0.1, max_tokens=400)
    except Exception as e:
        logger.warning("[L2] LLM formatting failed: %s", e)
        answer = summary_text

    return {
        "route": "PROFILE",
        "format_bucket": format_bucket,
        "answer": answer,
        "pyspark": None, "results": None,
    }
