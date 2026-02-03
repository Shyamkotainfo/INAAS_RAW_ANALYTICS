# Backend/execution/executor_factory.py

from config.settings import settings
from execution.local_pyspark_executor import LocalPySparkExecutor
# future
# from execution.databricks_executor import DatabricksExecutor
# from execution.emr_executor import EMRExecutor


def get_executor():
    if settings.execution_mode == "local":
        return LocalPySparkExecutor()

    if settings.execution_mode == "databricks":
        return DatabricksExecutor()

    if settings.execution_mode == "emr":
        return EMRExecutor()

    raise ValueError("Invalid execution mode")
