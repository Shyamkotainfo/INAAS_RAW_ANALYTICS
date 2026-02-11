# schema_extractor.py (Databricks)

from pyspark.sql import SparkSession
from datetime import datetime, timezone
import json

spark = SparkSession.builder.getOrCreate()

FILE_ID = "employee_details"
FILE_PATH = "/Volumes/inaas_dev/hr_analytics/employee_data/employee_details.csv"
FILE_FORMAT = "csv"

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(FILE_PATH)
)

columns = []
for field in df.schema.fields:
    samples = (
        df.select(field.name)
        .where(df[field.name].isNotNull())
        .limit(2)
        .collect()
    )

    columns.append({
        # 🔑 critical for chunk-level retrieval
        "file_id": FILE_ID,
        "file_path": FILE_PATH,

        "name": field.name,
        "type": field.dataType.simpleString(),
        "nullable": field.nullable,
        "sample_values": [str(r[0]) for r in samples]
    })

schema = {
    "file_id": FILE_ID,
    "data_location": {
        "type": "databricks_volume",
        "path": FILE_PATH
    },
    "format": FILE_FORMAT,
    "columns": columns,
    "ingested_at": datetime.now(timezone.utc).isoformat()
}

# ⚠️ strict marker for backend parser
print("INAAS_SCHEMA_JSON=" + json.dumps(schema))
