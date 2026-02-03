# Backend/execution/local_pyspark_executor.py

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

from spark.spark_session import get_spark
from execution.dataframe_loader import load_dataframe
from execution.base_executor import BaseExecutor


class LocalPySparkExecutor(BaseExecutor):

    def __init__(self):
        self.spark = get_spark()

    # ------------------------------
    # Shared loader
    # ------------------------------
    def load_df(self, file_path: str) -> DataFrame:
        return load_dataframe(self.spark, file_path)

    # ------------------------------
    # Query execution (LLM code)
    # ------------------------------
    def execute_query(self, file_path: str, pyspark_code: str) -> list[dict]:

        df = self.load_df(file_path)

        exec_context = {
            "df": df,
            "F": F,
        }

        try:
            exec(pyspark_code, {}, exec_context)
        except Exception as e:
            raise RuntimeError(
                f"Error while executing generated PySpark code:\n\n{pyspark_code}"
            ) from e

        if "result_df" not in exec_context:
            raise ValueError("Generated code must define `result_df`")

        result_df = exec_context["result_df"]

        if not isinstance(result_df, DataFrame):
            raise TypeError("result_df must be a DataFrame")

        return [row.asDict() for row in result_df.limit(100).collect()]

    # ------------------------------
    # Profile (Spark-only)
    # ------------------------------
    def execute_profile(self, file_path: str) -> dict:
        raise NotImplementedError("Profile handled in orchestrator")

    # ------------------------------
    # Quality (Spark-only)
    # ------------------------------
    def execute_quality(self, file_path: str) -> dict:
        raise NotImplementedError("Quality handled in orchestrator")
