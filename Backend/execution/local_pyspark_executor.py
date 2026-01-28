# Backend/execution/local_pyspark_executor.py

from pyspark.sql import functions as F
from ingestion.spark_session import get_spark
from execution.dataframe_loader import load_dataframe


class LocalPySparkExecutor:
    def __init__(self):
        self.spark = get_spark()

    def execute(self, file_path: str, pyspark_code: str):
        df = load_dataframe(self.spark, file_path)

        exec_context = {
            "df": df,
            "F": F
        }

        exec(pyspark_code, {}, exec_context)

        if "result_df" not in exec_context:
            raise ValueError("Generated code must define `result_df`")

        result_df = exec_context["result_df"]
        return [row.asDict() for row in result_df.limit(100).collect()]

