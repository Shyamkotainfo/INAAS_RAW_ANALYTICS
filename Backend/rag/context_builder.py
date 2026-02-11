import json
import re
from logger.logger import get_logger

logger = get_logger(__name__)


def build_query_context(chunks: list[str]) -> dict:
    """
    Build execution context from KB chunks.

    Expected to extract:
    - file_path
    - format
    - columns: [{name, type}]
    """

    context = {
        "file_path": None,
        "format": "csv",
        "columns": []
    }

    for chunk in chunks:
        # ---------- Try JSON first ----------
        try:
            data = json.loads(chunk)

            # file path
            if not context["file_path"]:
                if "file_path" in data:
                    context["file_path"] = data["file_path"]
                elif "data_location" in data:
                    context["file_path"] = data["data_location"].get("path")

            # format
            if "format" in data:
                context["format"] = data["format"]

            # columns
            if "columns" in data:
                for c in data["columns"]:
                    if "name" in c:
                        context["columns"].append({
                            "name": c["name"],
                            "type": c.get("type", "string")
                        })

            continue

        except Exception:
            pass  # Not JSON, fall back to text parsing

        # ---------- Fallback: regex-based extraction ----------
        if not context["file_path"]:
            m = re.search(r"/Volumes/[^\s\"']+", chunk)
            if m:
                context["file_path"] = m.group(0)

        # column names like: "name": "EmpCode"
        for match in re.findall(r'"name"\s*:\s*"([^"]+)"', chunk):
            context["columns"].append({
                "name": match,
                "type": "string"
            })

    # ---------- Final validation ----------
    if not context["file_path"]:
        raise RuntimeError(
            "file_path could not be inferred from KB chunks"
        )

    # Deduplicate columns
    seen = set()
    unique_cols = []
    for c in context["columns"]:
        if c["name"] not in seen:
            seen.add(c["name"])
            unique_cols.append(c)

    context["columns"] = unique_cols

    if not context["columns"]:
        raise RuntimeError("No columns available in query context")

    logger.info(
        "Query context built | file_path=%s | columns=%d",
        context["file_path"],
        len(context["columns"])
    )

    return context
