from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from ingestion.spark_session import get_spark
from execution.dataframe_loader import load_dataframe


class LocalPySparkExecutor:
    def __init__(self):
        self.spark = get_spark()

    def execute(self, file_path: str, pyspark_code: str):
        # Load dataframe
        df = load_dataframe(self.spark, file_path)

        # Execution sandbox
        exec_context = {
            "df": df,
            "F": F
        }

        # Execute generated PySpark code
        try:
            exec(pyspark_code, {}, exec_context)
        except Exception as e:
            raise RuntimeError(
                f"Error while executing generated PySpark code:\n\n{pyspark_code}"
            ) from e

        # Contract enforcement
        if "result_df" not in exec_context:
            raise ValueError(
                "Generated code must define a variable named `result_df`"
            )

        result_df = exec_context["result_df"]

        # HARD SAFETY CHECK (very important)
        if not isinstance(result_df, DataFrame):
            raise TypeError(
                f"Generated code returned {type(result_df)}. "
                "Expected pyspark.sql.DataFrame.\n\n"
                f"Generated code:\n{pyspark_code}"
            )

        # Return JSON-serializable output
        return [row.asDict() for row in result_df.limit(100).collect()]
