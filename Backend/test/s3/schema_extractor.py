from pyspark.sql import SparkSession
from datetime import datetime, timezone
import boto3
import json

# ---------------- Spark ----------------
spark = SparkSession.builder.getOrCreate()

# ---------------- Config ----------------
BUCKET = "inaas-raw-data"
RAW_PREFIX = "data-files/"
METADATA_BUCKET = "inaas-raw-analytics-dev"

SUPPORTED_FORMATS = {"csv", "parquet", "json", "orc", "avro"}

# ---------------- AWS ----------------
s3 = boto3.client("s3")

# ---------------- List Files ----------------
response = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix=RAW_PREFIX
)

objects = response.get("Contents", [])
data_files = []

for obj in objects:
    key = obj["Key"]

    # Skip folder placeholders
    if key.endswith("/"):
        continue

    # Must have extension
    if "." not in key:
        continue

    ext = key.rsplit(".", 1)[-1].lower()
    if ext in SUPPORTED_FORMATS:
        data_files.append(key)

if not data_files:
    raise RuntimeError("No supported data files found under datafiles/")

# Pick first file (extend later)
file_key = data_files[0]
file_format = file_key.rsplit(".", 1)[-1].lower()
file_path = f"s3a://{BUCKET}/{file_key}"

print(f"Processing file: {file_path}")

# ---------------- Read Data ----------------
if file_format == "csv":
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(file_path)
    )
elif file_format == "json":
    df = spark.read.json(file_path)
elif file_format == "parquet":
    df = spark.read.parquet(file_path)
elif file_format == "orc":
    df = spark.read.orc(file_path)
elif file_format == "avro":
    df = spark.read.format("avro").load(file_path)
else:
    raise ValueError(f"Unsupported file format: {file_format}")

# ---------------- Schema ----------------
columns = []
for field in df.schema.fields:
    samples = (
        df.select(field.name)
        .filter(df[field.name].isNotNull())
        .limit(2)
        .collect()
    )

    columns.append({
        "file_path": file_path,
        "name": field.name,
        "type": str(field.dataType),
        "sample_values": [str(r[0]) for r in samples]
    })

metadata = {
    "file_id": file_path,
    "file_path": file_path,
    "format": file_format,
    "columns": columns,
    "row_count": df.count(),
    "ingested_at": datetime.now(timezone.utc).isoformat()
}

# ---------------- Write Metadata ----------------
base_name = file_key.split("/")[-1].rsplit(".", 1)[0]
metadata_key = f"schema/{base_name}.json"

s3.put_object(
    Bucket=METADATA_BUCKET,
    Key=metadata_key,
    Body=json.dumps(metadata, indent=2),
    ContentType="application/json"
)

print(f"Schema extraction completed → s3://{METADATA_BUCKET}/{metadata_key}")
