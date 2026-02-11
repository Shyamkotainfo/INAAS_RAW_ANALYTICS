from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import sys

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# Read input payload
# -----------------------------
payload = json.loads(sys.stdin.read())

file_path = payload["file_path"]
file_format = payload["format"]
pyspark_code = payload["pyspark_code"]

# -----------------------------
# Load DataFrame
# -----------------------------
if file_format == "csv":
    df = spark.read.option("header", "true").csv(file_path)
elif file_format == "parquet":
    df = spark.read.parquet(file_path)
elif file_format == "json":
    df = spark.read.json(file_path)
else:
    raise ValueError(f"Unsupported format: {file_format}")

# -----------------------------
# Execute generated code
# -----------------------------
local_vars = {"df": df, "F": F}
exec(pyspark_code, {}, local_vars)

final_df = local_vars.get("final_df")

print("INAAS_RESULT_START")

if final_df is None:
    print("NO_RESULT")
else:
    rows = final_df.collect()
    if not rows:
        print("EMPTY_RESULT")
    else:
        for row in rows:
            print(row.asDict())

print("INAAS_RESULT_END")
