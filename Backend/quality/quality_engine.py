# Backend/quality/quality_engine.py

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from quality.rules import (
    INVALID_TOKENS,
    NULL_THRESHOLD_WARN,
    NULL_THRESHOLD_FAIL,
    DUPLICATE_THRESHOLD,
)
from quality.quality_report import empty_report


def run_quality_checks(df: DataFrame, primary_key: str = "EmpCode") -> dict:
    report = empty_report()

    total_rows = df.count()

    report["dataset"] = {
        "row_count": total_rows,
        "column_count": len(df.columns),
    }

    if total_rows == 0:
        report["status"] = "FAIL"
        report["issues"].append("Dataset is empty")
        return report

    # -------------------------------
    # Primary key duplicate check
    # -------------------------------
    if primary_key in df.columns:
        dup_count = (
            df.groupBy(primary_key)
              .count()
              .filter(F.col("count") > 1)
              .count()
        )

        if dup_count > DUPLICATE_THRESHOLD:
            report["status"] = "WARN"
            report["issues"].append(
                f"{primary_key} has {dup_count} duplicate values"
            )

    # -------------------------------
    # Column-level checks
    # -------------------------------
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = null_count / total_rows

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
            report["status"] = "FAIL"
            report["issues"].append(
                f"{col} has critical null rate ({null_pct:.1%})"
            )

        elif null_pct > NULL_THRESHOLD_WARN:
            col_status = "WARN"
            if report["status"] != "FAIL":
                report["status"] = "WARN"
            report["issues"].append(
                f"{col} has high null rate ({null_pct:.1%})"
            )

        if invalid_count > 0:
            if report["status"] == "PASS":
                report["status"] = "WARN"
            report["issues"].append(
                f"{col} contains {invalid_count} invalid placeholder values"
            )

        report["columns"].append({
            "column": col,
            "null_percentage": round(null_pct, 3),
            "distinct_count": distinct_count,
            "invalid_values": invalid_count,
            "status": col_status
        })

    return report
