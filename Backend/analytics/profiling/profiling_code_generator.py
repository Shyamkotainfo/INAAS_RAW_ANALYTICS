# Backend/analytics/profiling/profiling_code_generator.py


class ProfilingCodeGenerator:
    """
    Generates full PySpark code for dataset profiling.

    - Runs inside Databricks.
    - Compatible with: exec(pyspark_code, {}, local_vars)
    - Does NOT require additional imports.
    - Produces `profiling_output` dictionary.
    """

    def __init__(self, sample_size: int = 5):
        self.sample_size = sample_size

    def generate(self) -> str:
        return f"""
# ============================================
# INAAS DATASET PROFILING
# ============================================

profile = {{}}

# --------------------------------------------
# Dataset Level Profiling
# --------------------------------------------
total_rows = df.count()

profile["dataset"] = {{
    "row_count": total_rows,
    "column_count": len(df.columns),
    "columns": df.columns
}}

# --------------------------------------------
# Column Level Profiling
# --------------------------------------------
column_profiles = []

for field in df.schema.fields:
    col_name = field.name
    col_type = field.dataType
    col_type_str = str(col_type).lower()

    null_count = df.filter(F.col(col_name).isNull()).count()
    distinct_count = df.select(col_name).distinct().count()

    samples = (
        df.select(col_name)
          .dropna()
          .limit({self.sample_size})
          .rdd.flatMap(lambda x: x)
          .collect()
    )

    col_profile = {{
        "column": col_name,
        "data_type": str(col_type),
        "null_count": null_count,
        "null_percentage": round((null_count / total_rows) * 100, 2) if total_rows > 0 else 0,
        "distinct_count": distinct_count,
        "sample_values": [str(s) for s in samples]
    }}

    # ----------------------------------------
    # Safe numeric/date detection
    # (no generator expression)
    # ----------------------------------------
    if (
        "int" in col_type_str
        or "double" in col_type_str
        or "float" in col_type_str
        or "long" in col_type_str
        or "date" in col_type_str
        or "timestamp" in col_type_str
    ):
        min_max = df.select(
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max")
        ).first()

        col_profile["min"] = str(min_max["min"])
        col_profile["max"] = str(min_max["max"])

    column_profiles.append(col_profile)

profile["columns"] = column_profiles

# --------------------------------------------
# Required by INAAS executor
# --------------------------------------------
final_df = df
profiling_output = profile
"""
