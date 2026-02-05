from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("INAAS-EMR-Serverless-Test")
        .getOrCreate()
    )

    # -------------------------------
    # Simple in-memory test DataFrame
    # -------------------------------
    data = [
        ("Alice", 30),
        ("Bob", 25),
        ("Charlie", 35),
    ]

    df = spark.createDataFrame(data, ["name", "age"])

    # Simple transformation
    result_df = df.groupBy().avg("age")

    # Trigger execution
    result = result_df.collect()

    print("=== EMR SERVERLESS TEST RESULT ===")
    print(result)

    spark.stop()


if __name__ == "__main__":
    main()
