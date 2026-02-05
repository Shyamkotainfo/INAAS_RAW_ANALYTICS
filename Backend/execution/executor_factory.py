# Backend/execution/executor_factory.py

from config.settings import settings
from execution.local_pyspark_executor import LocalPySparkExecutor
from execution.emr_serverless_executor import EMRServerlessExecutor


def get_executor():
    if settings.execution_mode == "local":
        return LocalPySparkExecutor()

    if settings.execution_mode == "databricks":
        return DatabricksExecutor()

    if settings.execution_mode == "emr":
        return EMRServerlessExecutor()

    raise ValueError("Invalid execution mode")
