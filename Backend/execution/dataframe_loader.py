# Backend/execution/dataframe_loader.py

from pyspark.sql import SparkSession


def load_dataframe(spark: SparkSession, file_path: str):
    path = file_path.lower()

    if path.endswith(".csv"):
        return (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(file_path)
        )

    if path.endswith(".parquet"):
        return spark.read.parquet(file_path)

    if path.endswith(".json"):
        return spark.read.option("multiline", "true").json(file_path)

    raise ValueError(f"Unsupported file format: {file_path}")
