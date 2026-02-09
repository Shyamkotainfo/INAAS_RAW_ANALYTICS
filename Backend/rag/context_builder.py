import json
import re
from typing import List, Dict


S3_PATH_REGEX = re.compile(r"s3a://[a-zA-Z0-9\-_/\.]+")
FORMAT_REGEX = re.compile(r"\b(csv|parquet|json|orc|avro)\b", re.IGNORECASE)


def build_query_context(chunks: List[str]) -> Dict:
    """
    Build execution context from Bedrock KB chunks.

    Bedrock returns unstructured text, NOT guaranteed JSON.
    We must extract signals defensively.
    """

    file_path = None
    file_format = None
    columns = {}

    for chunk in chunks:
        if not chunk:
            continue

        chunk = chunk.strip()

        # --------------------------------------------------
        # 1. Try JSON parsing (best case)
        # --------------------------------------------------
        try:
            data = json.loads(chunk)

            if isinstance(data, dict):
                file_path = file_path or data.get("file_path") or data.get("file_id")
                file_format = file_format or data.get("format")

                if "columns" in data:
                    for col in data["columns"]:
                        name = col.get("name")
                        if name:
                            columns[name] = {
                                "name": name,
                                "type": col.get("type", "string")
                            }

                continue
        except Exception:
            pass  # expected for KB chunks

        # --------------------------------------------------
        # 2. Extract S3 path from raw text
        # --------------------------------------------------
        if not file_path:
            match = S3_PATH_REGEX.search(chunk)
            if match:
                file_path = match.group(0)

        # --------------------------------------------------
        # 3. Extract file format
        # --------------------------------------------------
        if not file_format:
            match = FORMAT_REGEX.search(chunk)
            if match:
                file_format = match.group(1).lower()

        # --------------------------------------------------
        # 4. Heuristic column extraction
        # --------------------------------------------------
        # Looks for patterns like:
        # EmpCode StringType()
        # Name StringType()
        tokens = chunk.split()
        if len(tokens) >= 2 and "Type" in tokens[1]:
            col_name = tokens[0].strip(",")
            col_type = tokens[1]

            columns[col_name] = {
                "name": col_name,
                "type": col_type
            }

    # --------------------------------------------------
    # Final validation
    # --------------------------------------------------
    if not file_path:
        raise ValueError(
            "file_path could not be inferred from KB chunks. "
            "Ensure metadata contains s3a:// paths."
        )

    if not file_format:
        file_format = file_path.rsplit(".", 1)[-1].lower()

    return {
        "file_path": file_path,
        "format": file_format,
        "columns": list(columns.values())
    }
