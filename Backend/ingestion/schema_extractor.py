from datetime import datetime, UTC
from logger.logger import get_logger
from spark.spark_session import get_spark
from .s3_reader import S3Reader
from .metadata_uploader import MetadataUploader
from .bedrock_ingestor import BedrockIngestor

logger = get_logger(__name__)


class SchemaExtractor:
    def __init__(self):
        self.spark = get_spark()
        self.s3 = S3Reader()
        self.uploader = MetadataUploader()
        self.bedrock = BedrockIngestor()

    def run(self):
        logger.info("Starting schema extraction")

        files = self.s3.list_files()
        if not files:
            raise RuntimeError("No files found in raw bucket")

        file_key = files[0]
        s3_path = f"s3a://{self.s3.bucket}/{file_key}"
        file_format = file_key.split(".")[-1]

        logger.info(f"Processing file: {s3_path}")

        if file_format == "csv":
            df = self.spark.read.csv(
                s3_path, header=True, inferSchema=True
            )
        else:
            df = self.spark.read.format(file_format).load(s3_path)

        columns = []
        for field in df.schema.fields:
            samples = (
                df.select(field.name)
                .filter(df[field.name].isNotNull())
                .limit(2)
                .collect()
            )

            # 🔑 CRITICAL FIX: include file_path in every column chunk
            columns.append({
                "file_path": s3_path,
                "name": field.name,
                "type": str(field.dataType),
                "sample_values": [str(row[0]) for row in samples]
            })

        metadata = {
            "file_id": s3_path,
            "file_path": s3_path,   # 🔁 intentional duplication (safe for RAG)
            "format": file_format,
            "columns": columns,
            "row_count": df.count(),
            "s3_uploaded_time": self.s3.get_last_modified(file_key),
            "ingested_at": datetime.now(UTC).isoformat()
        }

        self.uploader.upload(metadata, file_key)
        self.bedrock.ingest()

        logger.info("Schema extraction completed successfully")


if __name__ == "__main__":
    SchemaExtractor().run()
