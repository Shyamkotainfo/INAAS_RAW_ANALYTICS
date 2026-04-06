#run_query.py
import sys
import json
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

else:
    raise ValueError(f"Unsupported format: {file_format}")

# --------------------------------------------
# Execute generated PySpark
# --------------------------------------------
# NOTE: column validation is intentionally NOT done here.
# The generated code operates on multiple DataFrames (df, intermediate
# aliased frames, etc.). Checking F.col() references only against df.columns
# produces false positives for fully valid derived columns.
# Spark raises a clear AnalysisException for genuine missing-column errors,
# which is caught below and surfaced via INAAS_EXECUTION_ERROR.
local_vars = {"df": df, "F": F, "Window": Window}

try:
    exec(pyspark_code, local_vars, local_vars)
except Exception as e:
    print("INAAS_EXECUTION_ERROR")
    print(str(e))
    raise

if "final_df" not in local_vars:
    raise RuntimeError("final_df not produced by generated code")

final_df = local_vars["final_df"]

# --------------------------------------------
# Serialize results
# --------------------------------------------
rows = final_df.limit(500).collect()
columns = final_df.columns

result = {
    "columns": columns,
    "rows": [
        [str(v) if v is not None else None for v in row]
        for row in rows
    ]
}

print("INAAS_RESULT:", json.dumps(result))
