#run_query.py
import sys
import json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

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
# Validate column usage
# --------------------------------------------
used_columns = re.findall(r'F\.col\("([^"]+)"\)', pyspark_code)
invalid = [c for c in used_columns if c not in df.columns]

if invalid:
    raise RuntimeError(f"Invalid columns referenced: {invalid}")

# --------------------------------------------
# Execute generated PySpark
# --------------------------------------------
local_vars = {"df": df, "F": F}

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
