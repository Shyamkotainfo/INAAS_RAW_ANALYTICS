# Backend/retrieval/context_builder.py

import re

FILE_PATH_REGEX = re.compile(r's3a://[^\s",]+')
COLUMN_NAME_REGEX = re.compile(r'"name"\s*:\s*"([^"]+)"')


def build_query_context(chunks: list[str]) -> dict:
    """
    Builds execution context from Bedrock KB chunks.

    Output:
    {
      file_id: str,
      columns: [{name}]
    }
    """

    file_path = None
    columns = set()

    for chunk in chunks:
        # 1️⃣ Resolve file path (once)
        if not file_path:
            match = FILE_PATH_REGEX.search(chunk)
            if match:
                file_path = match.group(0)

        # 2️⃣ Extract column names
        for col_match in COLUMN_NAME_REGEX.finditer(chunk):
            columns.add(col_match.group(1))

    if not file_path:
        raise ValueError(
            "File path could not be resolved from KB chunks. "
            "Ensure metadata includes file_path."
        )

    return {
        "file_id": file_path,
        "columns": [
            {"name": c, "type": "string"} for c in sorted(columns)
        ]
    }
