import sys
import json
import re
import base64
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

MAX_RESULT_ROWS = 500
MAX_RESULT_JSON_CHARS = 90000

# --------------------------------------------
# Parse input
# --------------------------------------------
args = json.loads(sys.argv[1])

file_path = args["file_path"]
file_format = args.get("format", "csv")
pyspark_code = args.get("pyspark_code")

if not pyspark_code:
    raise RuntimeError("pyspark_code is required for query execution")

spark = SparkSession.builder.getOrCreate()


def as_text(column_name):
    return F.trim(F.col(column_name).cast("string"))


def as_double(column_name):
    # Handles decimal values stored as strings like "93,97".
    cleaned = F.regexp_replace(as_text(column_name), ",", ".")
    return cleaned.cast("double")


def as_int(column_name):
    return as_double(column_name).cast("int")


def as_date(column_name):
    text_col = as_text(column_name)
    return F.coalesce(
        F.to_timestamp(text_col, "yyyy-MM-dd HH:mm:ss").cast("date"),
        F.to_timestamp(text_col, "yyyy-MM-dd'T'HH:mm:ss").cast("date"),
        F.to_date(text_col, "yyyy-MM-dd")
    )


def as_bool_flag(column_name):
    lowered = F.lower(as_text(column_name))
    return (
        F.when(lowered.isin("true", "1", "yes", "y"), F.lit(True))
        .when(lowered.isin("false", "0", "no", "n"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def as_priority_rank(column_name):
    lowered = F.lower(as_text(column_name))
    return (
        F.when(lowered == "high", F.lit(3))
        .when(lowered == "medium", F.lit(2))
        .when(lowered == "low", F.lit(1))
        .otherwise(F.lit(0))
    )


def emit_chunked_result(marker: str, payload: dict, chunk_size: int = 1000):
    encoded = base64.b64encode(
        json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
    ).decode("ascii")
    print(f"{marker}_BEGIN")
    for idx in range(0, len(encoded), chunk_size):
        print(f"{marker}_CHUNK:{encoded[idx:idx + chunk_size]}")
    print(f"{marker}_END")


def _serialize_rows(columns, rows):
    return {
        "columns": columns,
        "rows": [
            [str(v) if v is not None else None for v in row]
            for row in rows
        ]
    }


def build_safe_result(final_df):
    columns = final_df.columns
    limit = MAX_RESULT_ROWS
    rows = final_df.limit(limit).collect()
    result = _serialize_rows(columns, rows)
    encoded_len = len(json.dumps(result, ensure_ascii=True, separators=(",", ":")))

    while encoded_len > MAX_RESULT_JSON_CHARS and limit > 25:
        limit = max(25, limit // 2)
        rows = final_df.limit(limit).collect()
        result = _serialize_rows(columns, rows)
        encoded_len = len(json.dumps(result, ensure_ascii=True, separators=(",", ":")))

    if encoded_len > MAX_RESULT_JSON_CHARS:
        trimmed_columns = columns[: min(len(columns), 12)]
        slim_df = final_df.select(*trimmed_columns)
        rows = slim_df.limit(limit).collect()
        result = _serialize_rows(trimmed_columns, rows)
        encoded_len = len(json.dumps(result, ensure_ascii=True, separators=(",", ":")))

    result["result_metadata"] = {
        "returned_rows": len(result["rows"]),
        "payload_chars": encoded_len,
        "row_limit_applied": len(result["rows"]) < MAX_RESULT_ROWS,
    }
    return result

# --------------------------------------------
# Load dataframe
# --------------------------------------------
if file_format == "csv":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

elif file_format == "parquet":
    df = spark.read.parquet(file_path)

elif file_format == "json":
    df = (
        spark.read
        .option("multiLine", "true")
        .option("inferSchema", "true")
        .json(file_path)
    )
##
else:
    raise ValueError(f"Unsupported format: {file_format}")

# --------------------------------------------
# Validate column usage
# --------------------------------------------
used_columns = re.findall(r'F\.col\("([^"]+)"\)', pyspark_code)
aliased_columns = re.findall(r'\.alias\("([^"]+)"\)', pyspark_code)
derived_columns = re.findall(r'\.withColumn\("([^"]+)"', pyspark_code)
known_columns = set(df.columns) | set(aliased_columns) | set(derived_columns)
normalized_used_columns = [c.split(".")[-1] for c in used_columns]
invalid = sorted({c for c in normalized_used_columns if c not in known_columns})

if invalid:
    print(f"INAAS_VALIDATION_WARNING: unresolved column refs detected by static validator: {invalid}")

# --------------------------------------------
# Execute generated PySpark
# --------------------------------------------
local_vars = {
    "df": df,
    "F": F,
    "as_text": as_text,
    "as_double": as_double,
    "as_int": as_int,
    "as_date": as_date,
    "as_bool_flag": as_bool_flag,
    "as_priority_rank": as_priority_rank,
}

try:
    exec(pyspark_code, local_vars, local_vars)
except Exception as e:
    print("INAAS_EXECUTION_ERROR")
    print(str(e))
    raise

if "final_df" in local_vars:
    final_df = local_vars["final_df"]
elif "result_df" in local_vars:
    final_df = local_vars["result_df"]
else:
    raise RuntimeError("final_df not produced by generated code")

# --------------------------------------------
# Serialize results
# --------------------------------------------
result = build_safe_result(final_df)

emit_chunked_result("INAAS_RESULT", result)
