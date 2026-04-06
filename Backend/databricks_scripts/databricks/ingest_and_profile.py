import sys
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

args = json.loads(sys.argv[1])

file_id = args["file_id"]
file_path = args["file_path"]
file_format = args["format"]

print("INAAS_INGEST_START")
print(f"FILE_ID={file_id}")
print(f"FILE_PATH={file_path}")
print(f"FORMAT={file_format}")

# ---------------------------------------------------------
# Detect CSV delimiter
# ---------------------------------------------------------
def detect_delimiter(path):
    delimiters = [",", ";", "|", "\t"]

    try:
        local_path = path.replace("dbfs:", "/dbfs")

        with open(local_path, "r", encoding="utf-8") as f:
            header = f.readline()

        counts = {d: header.count(d) for d in delimiters}

        return max(counts, key=counts.get)

    except:
        return ","


# ---------------------------------------------------------
# Load Data
# ---------------------------------------------------------
if file_format == "csv":

    delimiter = detect_delimiter(file_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("delimiter", delimiter)
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


# ---------------------------------------------------------
# Cache dataset (major speed improvement)
# ---------------------------------------------------------
df = df.cache()

# ---------------------------------------------------------
# Dataset level metrics
# ---------------------------------------------------------
row_count = df.count()
column_count = len(df.columns)

duplicate_rows = row_count - df.dropDuplicates().count()

# ---------------------------------------------------------
# Sample rows (consistent for UI)
# ---------------------------------------------------------
sample_rows = df.limit(5).collect()

# ---------------------------------------------------------
# Null counts (single pass aggregation)
# ---------------------------------------------------------
null_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
]

null_counts_row = df.select(null_exprs).collect()[0]
null_counts = {c: null_counts_row[c] for c in df.columns}

# ---------------------------------------------------------
# Distinct counts (fast approx algorithm)
# ---------------------------------------------------------
distinct_exprs = [
    F.approx_count_distinct(c).alias(c)
    for c in df.columns
]

distinct_row = df.select(distinct_exprs).collect()[0]
distinct_counts = {c: distinct_row[c] for c in df.columns}

profiling = {
    "row_count": row_count,
    "column_count": column_count,
    "duplicate_rows": duplicate_rows,
    "columns": []
}

schema_columns = []

# ---------------------------------------------------------
# Supported date formats
# ---------------------------------------------------------
date_formats = [
    "yyyy-MM-dd",
    "dd-MM-yyyy",
    "MM-dd-yyyy",
    "yyyy/MM/dd",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "dd.MM.yyyy"
]

# ---------------------------------------------------------
# Column profiling
# ---------------------------------------------------------
for field in df.schema.fields:

    col_name = field.name
    col_type = field.dataType.simpleString()
    col_type_lower = col_type.lower()
    nullable = field.nullable

    null_count = null_counts[col_name]
    distinct_count = distinct_counts[col_name]

    null_percentage = (
        round((null_count / row_count) * 100, 2)
        if row_count > 0 else 0
    )

    # -----------------------------------------------------
    # Sample values from same rows
    # -----------------------------------------------------
    sample_values = []

    for row in sample_rows:
        val = row[col_name]
        sample_values.append(str(val) if val is not None else None)

    column_profile = {
        "column_name": col_name,
        "data_type": col_type,
        "nullable": nullable,
        "null_count": null_count,
        "null_percentage": null_percentage,
        "distinct_count": distinct_count,
        "sample_values": sample_values
    }

    min_value = None
    max_value = None
    mean_value = None

    # -----------------------------------------------------
    # DATE DETECTION
    # -----------------------------------------------------
    date_detected = False

    for fmt in date_formats:

        df_date = df.withColumn(
            "_temp_date",
            F.to_date(F.col(col_name), fmt)
        )

        valid_dates = df_date.filter(
            F.col("_temp_date").isNotNull()
        ).count()

        if row_count > 0 and valid_dates / row_count > 0.7:

            stats = df_date.select(
                F.min("_temp_date").alias("min"),
                F.max("_temp_date").alias("max")
            ).first()

            column_profile["data_type"] = "date"

            min_value = stats["min"]
            max_value = stats["max"]

            date_detected = True
            break

    # -----------------------------------------------------
    # NUMERIC DETECTION
    # -----------------------------------------------------
    if not date_detected:

        numeric_types = ["int", "double", "float", "long", "decimal"]

        if any(t in col_type_lower for t in numeric_types):

            stats = df.select(
                F.min(col_name).alias("min"),
                F.max(col_name).alias("max"),
                F.avg(col_name).alias("mean")
            ).first()

            min_value = stats["min"]
            max_value = stats["max"]

            # detect year column
            year_check = df.filter(
                (F.col(col_name) >= 1900) &
                (F.col(col_name) <= 2100)
            ).count()

            if row_count > 0 and year_check / row_count < 0.9:
                mean_value = stats["mean"]

        else:

            metric_keywords = ["salary", "pay", "amount", "price", "cost"]

            if any(k in col_name.lower() for k in metric_keywords):

                df_numeric = df.withColumn(
                    "_temp_numeric",
                    F.regexp_extract(
                        F.col(col_name),
                        r"([-+]?[0-9]*\.?[0-9]+)",
                        0
                    ).cast("double")
                )

                numeric_count = df_numeric.filter(
                    F.col("_temp_numeric").isNotNull()
                ).count()

                if row_count > 0 and numeric_count / row_count > 0.7:

                    stats = df_numeric.select(
                        F.min("_temp_numeric").alias("min"),
                        F.max("_temp_numeric").alias("max"),
                        F.avg("_temp_numeric").alias("mean")
                    ).first()

                    min_value = stats["min"]
                    max_value = stats["max"]
                    mean_value = stats["mean"]

    # -----------------------------------------------------
    # Attach stats
    # -----------------------------------------------------
    if min_value is not None:
        column_profile["min"] = str(min_value)

    if max_value is not None:
        column_profile["max"] = str(max_value)

    if mean_value is not None:
        column_profile["mean"] = round(float(mean_value), 4)

    profiling["columns"].append(column_profile)

    schema_columns.append({
        "file_id": file_id,
        "file_path": file_path,
        "name": col_name,
        "type": column_profile["data_type"],
        "nullable": nullable,
        "sample_values": [],
        "description": None
    })

# ---------------------------------------------------------
# Schema object
# ---------------------------------------------------------
schema = {
    "file_id": file_id,
    "data_location": {
        "type": "databricks_volume",
        "path": file_path
    },
    "format": file_format,
    "columns": schema_columns,
    "ingested_at": datetime.now(timezone.utc).isoformat()
}

# ---------------------------------------------------------
# Output
# ---------------------------------------------------------
print("INAAS_SCHEMA_JSON=" + json.dumps(schema))
print("INAAS_PROFILING=" + json.dumps(profiling))
print("INAAS_INGEST_END")