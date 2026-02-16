import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ------------------------------------------------
# Parse input
# ------------------------------------------------
args = json.loads(sys.argv[1])

file_path = args["file_path"]
file_format = args.get("format", "csv")
pyspark_code = args["pyspark_code"]

print("INAAS_RUN_QUERY_START")
print(f"FILE_PATH={file_path}")
print("PYSPARK_CODE:")
print(pyspark_code)

# ------------------------------------------------
# Spark session
# ------------------------------------------------
spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------
# Load dataframe
# ------------------------------------------------
if file_format == "csv":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )
elif file_format == "parquet":
    df = spark.read.parquet(file_path)
else:
    raise ValueError(f"Unsupported format: {file_format}")

# ------------------------------------------------
# Execute generated PySpark code
# ------------------------------------------------
local_vars = {
    "df": df,
    "F": F
}

try:
    exec(pyspark_code, {}, local_vars)
except Exception as e:
    print("INAAS_EXECUTION_ERROR")
    print(str(e))
    raise


# ------------------------------------------------
# 1️⃣ PROFILING MODE
# ------------------------------------------------
if "profiling_output" in local_vars:
    print("INAAS_PROFILING:", json.dumps(local_vars["profiling_output"]))
    print("INAAS_RUN_QUERY_END")
    sys.exit(0)


# ------------------------------------------------
# 2️⃣ QUALITY MODE
# ------------------------------------------------
if "quality_output" in local_vars:
    print("INAAS_QUALITY:", json.dumps(local_vars["quality_output"]))
    print("INAAS_RUN_QUERY_END")
    sys.exit(0)


# ------------------------------------------------
# 3️⃣ NORMAL QUERY MODE
# ------------------------------------------------
if "final_df" not in local_vars:
    raise RuntimeError("final_df not produced by generated code")

final_df = local_vars["final_df"]


# ------------------------------------------------
# JSON-safe result serialization
# ------------------------------------------------
def _json_safe(value):
    if value is None:
        return None
    return str(value)


rows = final_df.limit(100).collect()
columns = final_df.columns

safe_rows = []
for r in rows:
    safe_rows.append([_json_safe(v) for v in r])


result = {
    "columns": columns,
    "rows": safe_rows
}

print("INAAS_RESULT:", json.dumps(result))
print("INAAS_RUN_QUERY_END")
