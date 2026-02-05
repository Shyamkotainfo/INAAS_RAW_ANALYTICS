import sys
import json
from pyspark.sql import SparkSession

def log(msg):
    print(f"[EMR-DEBUG] {msg}", flush=True)

def main():
    log(f"RAW sys.argv = {sys.argv}")

    if len(sys.argv) < 4:
        raise RuntimeError(f"Invalid arguments received: {sys.argv}")

    mode = sys.argv[1]
    # file_path = sys.argv[2]
    file_path = "s3://inaas-raw-data/data-files/employee_details.csv"

    log(f"Parsed mode      = {mode}")
    log(f"Parsed file_path = {file_path}")

    spark = SparkSession.builder.appName("INAAS-EMR").getOrCreate()

    log("About to read input file")
    log(f"spark.read.csv({file_path})")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )

    log("Input DataFrame loaded successfully")

    if mode == "query":
        if len(sys.argv) < 5:
            raise RuntimeError(f"Query mode expects code_path and job_id: {sys.argv}")

        code_path = sys.argv[3]
        job_id = sys.argv[4]

        log(f"Query mode detected")
        log(f"Code path = {code_path}")
        log(f"Job ID    = {job_id}")

        log("Reading generated PySpark code from S3")
        code_lines = spark.sparkContext.textFile(code_path).collect()
        code = "\n".join(code_lines)

        log("Executing generated PySpark code")

        exec_context = {"df": df}
        exec(code, {}, exec_context)

        if "result_df" not in exec_context:
            raise RuntimeError("Generated code did NOT define result_df")

        result_df = exec_context["result_df"]

        output_path = f"s3://inaas-raw-analytics-dev/emr/results/{job_id}/"

        log(f"Writing result to {output_path}")
        result_df.limit(100).write.mode("overwrite").json(output_path)

        log("Query execution completed successfully")

    else:
        raise RuntimeError(f"Unsupported mode: {mode}")

if __name__ == "__main__":
    main()
