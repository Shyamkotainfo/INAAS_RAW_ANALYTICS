# Backend/layers/metadata.py

import re
from logger.logger import get_logger
from llm.llm_query import invoke_llm
from layers.common import UNSTRUCTURED, load_metadata_from_s3

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Heuristics for dimension / measure classification
# ---------------------------------------------------------------------------

_MEASURE_DTYPES = {
    "int", "integer", "bigint", "smallint", "tinyint",
    "float", "double", "decimal", "numeric", "real",
    "long", "short", "number",
}

_DIMENSION_DTYPES = {
    "string", "varchar", "char", "text", "nvarchar",
    "boolean", "bool",
    "date", "timestamp", "datetime",
}

_MEASURE_NAME_PATTERNS = [
    r"\bpay\b", r"\bsalary\b", r"\bwage\b", r"\bcost\b", r"\bprice\b",
    r"\bamount\b", r"\brevenue\b", r"\bprofit\b", r"\bloss\b",
    r"\bcount\b", r"\bquantity\b", r"\bqty\b", r"\bunits\b",
    r"\bage\b", r"\btenure\b", r"\bscore\b", r"\brating\b",
    r"\brate\b", r"\bpercent\b", r"\bpct\b", r"\bratio\b",
    r"\byear\b", r"\bmonth\b", r"\bday\b",
    r"\bduration\b", r"\blatitude\b", r"\blongitude\b",
]

_DIMENSION_NAME_PATTERNS = [
    r"\bname\b", r"\bcode\b", r"\bid\b", r"\bkey\b", r"\btype\b",
    r"\bcategory\b", r"\bstatus\b", r"\bflag\b", r"\blabel\b",
    r"\bgender\b", r"\bdepartment\b", r"\bdesignation\b", r"\brole\b",
    r"\blocation\b", r"\bcity\b", r"\bstate\b", r"\bcountry\b",
    r"\bregion\b", r"\bzone\b", r"\bteam\b", r"\bdivision\b",
    r"\bqualification\b", r"\bcourse\b", r"\buniversity\b",
    r"\bemployment\b", r"\bcompany\b", r"\btitle\b",
]

_IDENTIFIER_NAME_PATTERNS = [
    r"^id$", r"_id$", r"code$", r"^emp", r"^cust", r"^order",
    r"^uuid", r"^guid", r"^pk", r"^fk",
]

_TIMESTAMP_NAME_PATTERNS = [
    r"\bdate\b", r"\bdoj\b", r"\bcreated\b", r"\bupdated\b",
    r"\btimestamp\b", r"\btime\b", r"\bjoined\b", r"\bstart\b", r"\bend\b",
]

def _classify_column(name: str, dtype: str) -> str:
    n = name.lower().strip()
    d = dtype.lower().strip()
    if any(re.search(p, n) for p in _IDENTIFIER_NAME_PATTERNS):
        return "IDENTIFIER"
    if d in ("date", "timestamp", "datetime") or any(re.search(p, n) for p in _TIMESTAMP_NAME_PATTERNS):
        return "TIMESTAMP"
    if d in _MEASURE_DTYPES:
        return "MEASURE"
    if d in _DIMENSION_DTYPES:
        return "DIMENSION"
    if any(re.search(p, n) for p in _MEASURE_NAME_PATTERNS):
        return "MEASURE"
    if any(re.search(p, n) for p in _DIMENSION_NAME_PATTERNS):
        return "DIMENSION"
    return "UNKNOWN"

def _build_column_classification(columns: list) -> dict:
    buckets = {
        "identifiers": [],
        "timestamps":  [],
        "dimensions":  [],
        "measures":    [],
        "unknown":     [],
    }
    for col in columns:
        name     = col.get("name", col.get("column_name", "?"))
        dtype    = col.get("type", col.get("data_type", "string"))
        nullable = col.get("nullable", True)
        role     = _classify_column(name, dtype)
        entry = {"name": name, "type": dtype, "nullable": nullable}
        bucket_key = {
            "IDENTIFIER": "identifiers",
            "TIMESTAMP":  "timestamps",
            "DIMENSION":  "dimensions",
            "MEASURE":    "measures",
            "UNKNOWN":    "unknown",
        }[role]
        buckets[bucket_key].append(entry)
    return buckets

def _format_bucket_summary(buckets: dict) -> str:
    lines = []
    role_labels = [
        ("identifiers", "🔑 Identifiers"),
        ("timestamps",  "📅 Timestamps"),
        ("dimensions",  "📂 Dimensions"),
        ("measures",    "📊 Measures"),
        ("unknown",     "❓ Unclassified (review needed)"),
    ]
    for key, label in role_labels:
        cols = buckets.get(key, [])
        if not cols:
            continue
        lines.append(f"\n{label} ({len(cols)}):")
        for c in cols:
            nullable_tag = "nullable" if c["nullable"] else "not null"
            lines.append(f"  • {c['name']}  [{c['type']}]  ({nullable_tag})")
    return "\n".join(lines)

def handle_metadata(question: str, dataset_id: str, format_bucket: str, active_file: dict = None) -> dict:
    """Answer schema/structural questions from stored metadata."""
    if format_bucket == UNSTRUCTURED:
        return {
            "route": "METADATA",
            "format_bucket": format_bucket,
            "answer": (
                "This is an unstructured document. Schema-level questions are not applicable. "
                "Try asking 'what is this document about?' instead."
            ),
            "pyspark": None, "results": None,
        }

    metadata = load_metadata_from_s3(dataset_id) or (active_file.get("schema", {}) if active_file else {})
    if not metadata:
        return None  # Signal fallback to ADHOC

    columns   = metadata.get("columns", [])
    file_fmt  = metadata.get("format", active_file.get("format", "unknown") if active_file else "unknown")
    file_path = metadata.get("data_location", {}).get("path", "")

    buckets        = _build_column_classification(columns)
    schema_summary = _format_bucket_summary(buckets)

    header = (
        f"Dataset : {file_path}\n"
        f"Format  : {file_fmt.upper()}\n"
        f"Columns : {len(columns)} total  |  "
        f"{len(buckets['identifiers'])} identifiers  |  "
        f"{len(buckets['timestamps'])} timestamps  |  "
        f"{len(buckets['dimensions'])} dimensions  |  "
        f"{len(buckets['measures'])} measures  |  "
        f"{len(buckets['unknown'])} unclassified"
    )

    raw_answer = f"{header}\n{schema_summary}"
    
    try:
        unknown_block = ""
        if buckets["unknown"]:
            unknown_list = "\n".join(f"  - {c['name']} [{c['type']}]" for c in buckets["unknown"])
            unknown_block = (
                f"\nThe following columns could not be auto-classified. "
                f"Please classify each as DIMENSION or MEASURE based on name and context:\n"
                f"{unknown_list}\n"
            )

        llm_prompt = (
            f"The user asked: \"{question}\"\n\n"
            f"Here is the dataset schema with column role classifications:\n"
            f"{raw_answer}\n"
            f"{unknown_block}\n"
            f"Instructions:\n"
            f"1. Answer the user's question using only the schema information above.\n"
            f"2. If the question is about dimensions or measures, refer to the classified buckets.\n"
            f"3. If there are unclassified columns, suggest their likely role (DIMENSION/MEASURE) "
            f"   with a brief reason.\n"
            f"4. Be concise and direct. Do not invent column names or data not shown above."
        )
        final_answer = invoke_llm(prompt=llm_prompt, temperature=0.1, max_tokens=600)
    except Exception as e:
        logger.warning("[L1] LLM formatting failed: %s", e)
        final_answer = raw_answer

    return {
        "route": "METADATA",
        "format_bucket": format_bucket,
        "answer": final_answer,
        "pyspark": None,
        "results": {
            "column_classification": buckets,
            "total_columns": len(columns),
            "path": file_path, "format": file_fmt,
        },
    }
