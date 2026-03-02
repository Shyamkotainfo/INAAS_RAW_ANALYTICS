import sys
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------
# Initialize Spark
# ---------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# ---------------------------------------------------------
# Parse input arguments
# ---------------------------------------------------------
args = json.loads(sys.argv[1])

file_id = args["file_id"]
file_path = args["file_path"]
file_format = args["format"]

print("INAAS_INGEST_START")
print(f"FILE_ID={file_id}")
print(f"FILE_PATH={file_path}")
print(f"FORMAT={file_format}")

# ---------------------------------------------------------
# Load DataFrame
# ---------------------------------------------------------
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

# ---------------------------------------------------------
# Dataset Level Profiling
# ---------------------------------------------------------
row_count = df.count()
column_count = len(df.columns)

profiling = {
    "row_count": row_count,
    "column_count": column_count,
    "columns": []
}

schema_columns = []

# ---------------------------------------------------------
# Column-Level Profiling
# ---------------------------------------------------------
for field in df.schema.fields:

    col_name = field.name
    col_type = field.dataType.simpleString()
    col_type_lower = col_type.lower()
    col_name_lower = col_name.lower()
    nullable = field.nullable

    null_count = df.filter(F.col(col_name).isNull()).count()
    distinct_count = df.select(F.col(col_name)).distinct().count()

    null_percentage = (
        round((null_count / row_count) * 100, 2)
        if row_count > 0 else 0
    )

    top_values_rows = (
        df.groupBy(F.col(col_name))
        .count()
        .orderBy(F.desc("count"))
        .limit(5)
        .collect()
    )

    top_values = [
        {
            "value": str(row[col_name]) if row[col_name] is not None else None,
            "count": row["count"]
        }
        for row in top_values_rows
    ]

    column_profile = {
        "column_name": col_name,
        "data_type": col_type,
        "nullable": nullable,
        "null_count": null_count,
        "null_percentage": null_percentage,
        "distinct_count": distinct_count,
        "top_values": top_values
    }

    # -----------------------------------------------------
    # SMART STATISTICAL DETECTION
    # -----------------------------------------------------
    min_value = None
    max_value = None
    mean_value = None

    # 1️⃣ Identifier detection (skip stats)
    identifier_keywords = ["id", "code", "emp", "number"]
    if any(keyword in col_name_lower for keyword in identifier_keywords):
        pass

    else:

        # 2️⃣ Date detection FIRST
        possible_formats = [
            "yyyy-MM-dd",
            "dd-MM-yyyy",
            "MM-dd-yyyy",
            "yyyy/MM/dd",
            "dd/MM/yyyy",
            "MM/dd/yyyy"
        ]

        date_detected = False

        for fmt in possible_formats:
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

                min_value = stats["min"]
                max_value = stats["max"]
                date_detected = True
                break

        # 3️⃣ Native numeric columns
        if not date_detected:

            numeric_types = ["int", "bigint", "double", "float", "long", "decimal"]

            if any(t in col_type_lower for t in numeric_types):

                stats = df.select(
                    F.min(col_name).alias("min"),
                    F.max(col_name).alias("max"),
                    F.avg(col_name).alias("mean")
                ).first()

                min_value = stats["min"]
                max_value = stats["max"]

                # Year detection → no mean
                year_check = df.filter(
                    (F.col(col_name) >= 1900) &
                    (F.col(col_name) <= 2100)
                ).count()

                if row_count > 0 and year_check / row_count < 0.9:
                    mean_value = stats["mean"]

            else:
                # 4️⃣ Numeric inside string (salary-like only)
                metric_keywords = ["salary", "pay", "amount", "price", "cost"]

                if any(k in col_name_lower for k in metric_keywords):

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

    # Attach stats safely
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
        "type": col_type,
        "nullable": nullable,
        "sample_values": [],
        "description": None
    })

# ---------------------------------------------------------
# Final Schema Object
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
# Output (single line for backend parsing)
# ---------------------------------------------------------
print("INAAS_SCHEMA_JSON=" + json.dumps(schema))
print("INAAS_PROFILING=" + json.dumps(profiling))
print("INAAS_INGEST_END")