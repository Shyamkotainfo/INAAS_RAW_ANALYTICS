# Backend/databricks_scripts/databricks/semantic_build_job.py
# Layer 3 — Semantic / KPI Layer Build Job
#
# Runs ONCE (or on schedule) to build gold aggregation tables.
# Results are pre-aggregated and written to S3 / Delta tables.
#
# Execution: Databricks Jobs API
# Output:    S3 path → semantic/{dataset_id}/kpi_<name>.parquet
#
# Pre-built KPIs (extend as needed):
#   - headcount_by_designation
#   - headcount_by_gender
#   - headcount_by_department
#   - avg_pay_by_designation
#   - avg_pay_by_gender
#   - hiring_trend_monthly
#   - attrition_by_department
#
# TODO (Phase C): Add more KPIs based on business requirements.
# TODO (Phase C): Wire into scheduled Databricks job.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

# ---------------------------------------------------------------------------
# Job params (passed via Databricks job parameters)
# ---------------------------------------------------------------------------
dbutils.widgets.text("file_path",     "", "S3 path to the raw dataset file")
dbutils.widgets.text("file_format",   "csv", "File format: csv | parquet | json")
dbutils.widgets.text("dataset_id",    "", "Unique dataset identifier")
dbutils.widgets.text("output_path",   "", "S3 base path to write KPI parquet files")
dbutils.widgets.text("pay_col",       "Pay",         "Column name for pay/salary")
dbutils.widgets.text("desig_col",     "Designation", "Column name for designation/role")
dbutils.widgets.text("gender_col",    "Gender",      "Column name for gender")
dbutils.widgets.text("dept_col",      "Department",  "Column name for department")
dbutils.widgets.text("doj_col",       "DOJ",         "Column name for date of joining")
dbutils.widgets.text("emp_col",       "EmpCode",     "Column name for employee ID")

file_path   = dbutils.widgets.get("file_path")
file_format = dbutils.widgets.get("file_format")
dataset_id  = dbutils.widgets.get("dataset_id")
output_path = dbutils.widgets.get("output_path")
PAY_COL     = dbutils.widgets.get("pay_col")
DESIG_COL   = dbutils.widgets.get("desig_col")
GENDER_COL  = dbutils.widgets.get("gender_col")
DEPT_COL    = dbutils.widgets.get("dept_col")
DOJ_COL     = dbutils.widgets.get("doj_col")
EMP_COL     = dbutils.widgets.get("emp_col")

assert file_path and dataset_id and output_path, "Missing required job parameters"

# ---------------------------------------------------------------------------
# Read raw data
# ---------------------------------------------------------------------------
spark = SparkSession.builder.getOrCreate()

if file_format == "csv":
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
elif file_format == "parquet":
    df = spark.read.parquet(file_path)
elif file_format == "json":
    df = spark.read.json(file_path)
else:
    raise ValueError(f"Unsupported file format: {file_format}")

base_out = f"{output_path}/{dataset_id}"

# ---------------------------------------------------------------------------
# Helper: write gold table
# ---------------------------------------------------------------------------
def write_kpi(kpi_df, kpi_name: str):
    path = f"{base_out}/{kpi_name}.parquet"
    kpi_df.write.mode("overwrite").parquet(path)
    print(f"  ✓ Written: {kpi_name} → {path}")


# ---------------------------------------------------------------------------
# KPI 1 — Headcount by Designation
# ---------------------------------------------------------------------------
if DESIG_COL in df.columns and EMP_COL in df.columns:
    write_kpi(
        df.groupBy(DESIG_COL).agg(F.count(EMP_COL).alias("headcount")),
        "headcount_by_designation"
    )

# ---------------------------------------------------------------------------
# KPI 2 — Headcount by Gender
# ---------------------------------------------------------------------------
if GENDER_COL in df.columns and EMP_COL in df.columns:
    write_kpi(
        df.groupBy(GENDER_COL).agg(F.count(EMP_COL).alias("headcount")),
        "headcount_by_gender"
    )

# ---------------------------------------------------------------------------
# KPI 3 — Headcount by Department
# ---------------------------------------------------------------------------
if DEPT_COL in df.columns and EMP_COL in df.columns:
    write_kpi(
        df.groupBy(DEPT_COL).agg(F.count(EMP_COL).alias("headcount")),
        "headcount_by_department"
    )

# ---------------------------------------------------------------------------
# KPI 4 — Average Pay by Designation
# ---------------------------------------------------------------------------
if DESIG_COL in df.columns and PAY_COL in df.columns:
    write_kpi(
        df.groupBy(DESIG_COL).agg(F.avg(PAY_COL).alias("avg_pay")),
        "avg_pay_by_designation"
    )

# ---------------------------------------------------------------------------
# KPI 5 — Average Pay by Gender
# ---------------------------------------------------------------------------
if GENDER_COL in df.columns and PAY_COL in df.columns:
    write_kpi(
        df.groupBy(GENDER_COL).agg(F.avg(PAY_COL).alias("avg_pay")),
        "avg_pay_by_gender"
    )

# ---------------------------------------------------------------------------
# KPI 6 — Hiring Trend (monthly headcount by DOJ month)
# ---------------------------------------------------------------------------
if DOJ_COL in df.columns and EMP_COL in df.columns:
    write_kpi(
        df.withColumn("hire_month", F.date_format(F.col(DOJ_COL), "yyyy-MM"))
          .groupBy("hire_month")
          .agg(F.count(EMP_COL).alias("hires"))
          .orderBy("hire_month"),
        "hiring_trend_monthly"
    )

# ---------------------------------------------------------------------------
# KPI 7 — Gender Ratio by Department
# ---------------------------------------------------------------------------
if DEPT_COL in df.columns and GENDER_COL in df.columns:
    total_by_dept = df.groupBy(DEPT_COL).agg(F.count("*").alias("total"))
    gender_by_dept = df.groupBy(DEPT_COL, GENDER_COL).agg(F.count("*").alias("count"))
    diversity = gender_by_dept.join(total_by_dept, on=DEPT_COL) \
                              .withColumn("ratio_pct", F.round(F.col("count") / F.col("total") * 100, 2))
    write_kpi(diversity, "gender_ratio_by_department")

print(f"\nSemantic build complete. All KPIs written to {base_out}/")
