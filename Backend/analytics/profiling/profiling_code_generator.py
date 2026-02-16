# Backend/analytics/profiling/profiling_code_generator.py


class ProfilingCodeGenerator:
    """
    Generates full PySpark code for dataset profiling.

    This code will run inside Databricks.
    """

    def __init__(self, sample_size: int = 5):
        self.sample_size = sample_size

    def generate(self) -> str:
        """
        Returns full PySpark code as string.
        """

        return f"""
from pyspark.sql.types import NumericType, DateType, TimestampType

profile = {{}}

# ----------------------------------------------------
# Dataset Level Profiling
# ----------------------------------------------------
total_rows = df.count()

profile["dataset"] = {{
    "row_count": total_rows,
    "column_count": len(df.columns),
    "columns": df.columns
}}

# ----------------------------------------------------
# Column Level Profiling
# ----------------------------------------------------
column_profiles = []

for field in df.schema.fields:
    col_name = field.name
    col_type = field.dataType

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

    if isinstance(col_type, (NumericType, DateType, TimestampType)):
        min_max = df.select(
            F.min(col_name).alias("min"),
            F.max(col_name).alias("max")
        ).first()

        col_profile["min"] = str(min_max["min"])
        col_profile["max"] = str(min_max["max"])

    column_profiles.append(col_profile)

profile["columns"] = column_profiles

# Required by INAAS executor
final_df = df
profiling_output = profile
"""
