# Backend/analytics/quality/quality_code_generator.py


class QualityCodeGenerator:
    """
    Generates full PySpark code for quality checks.
    This code runs inside Databricks.
    """

    def __init__(self, primary_key: str = "EmpCode"):
        self.primary_key = primary_key

    def generate(self) -> str:

        return f"""

INVALID_TOKENS = {{"NA", "N/A", "NULL", "", " "}}
NULL_THRESHOLD_WARN = 0.20
NULL_THRESHOLD_FAIL = 0.50
DUPLICATE_THRESHOLD = 0

total_rows = df.count()

quality_report = {{
    "status": "PASS",
    "dataset": {{
        "row_count": total_rows,
        "column_count": len(df.columns)
    }},
    "columns": [],
    "issues": []
}}

if total_rows == 0:
    quality_report["status"] = "FAIL"
    quality_report["issues"].append("Dataset is empty")

# ---------------------------------------------
# Primary Key Duplicate Check
# ---------------------------------------------
if "{self.primary_key}" in df.columns:
    dup_count = (
        df.groupBy("{self.primary_key}")
          .count()
          .filter(F.col("count") > 1)
          .count()
    )

    if dup_count > DUPLICATE_THRESHOLD:
        quality_report["status"] = "WARN"
        quality_report["issues"].append(
            f"{self.primary_key} has {{dup_count}} duplicate values"
        )

# ---------------------------------------------
# Column Level Checks
# ---------------------------------------------
for col in df.columns:

    null_count = df.filter(F.col(col).isNull()).count()
    null_pct = null_count / total_rows if total_rows > 0 else 0

    distinct_count = df.select(col).distinct().count()

    invalid_count = (
        df.filter(
            F.upper(F.trim(F.col(col))).isin(INVALID_TOKENS)
        ).count()
        if dict(df.dtypes)[col] == "string"
        else 0
    )

    col_status = "PASS"

    if null_pct > NULL_THRESHOLD_FAIL:
        col_status = "FAIL"
        quality_report["status"] = "FAIL"
        quality_report["issues"].append(
            f"{{col}} has critical null rate ({{null_pct:.1%}})"
        )

    elif null_pct > NULL_THRESHOLD_WARN:
        if quality_report["status"] != "FAIL":
            quality_report["status"] = "WARN"
        col_status = "WARN"
        quality_report["issues"].append(
            f"{{col}} has high null rate ({{null_pct:.1%}})"
        )

    if invalid_count > 0:
        if quality_report["status"] == "PASS":
            quality_report["status"] = "WARN"
        quality_report["issues"].append(
            f"{{col}} contains {{invalid_count}} invalid placeholder values"
        )

    quality_report["columns"].append({{
        "column": col,
        "null_percentage": round(null_pct, 3),
        "distinct_count": distinct_count,
        "invalid_values": invalid_count,
        "status": col_status
    }})

final_df = df
quality_output = quality_report
"""
