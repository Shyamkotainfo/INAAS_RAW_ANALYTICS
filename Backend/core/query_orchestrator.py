import json
from concurrent.futures import ThreadPoolExecutor

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from query_generation.pyspark_generator import PySparkCodeGenerator
from summarization.result_summarizer import ResultSummarizer
from classifier.query_classifier import QueryClassifier
from classifier.routing_schema import QueryRoute

# New modular layers
from layers.common import _detect_format_bucket, load_metadata_from_s3, load_profile_from_s3, STRUCTURED
from layers.metadata import handle_metadata
from layers.profile import handle_profile
from layers.kpi import handle_kpi
from layers.adhoc import AdhocLayer

logger = get_logger(__name__)

class QueryOrchestrator:

    def __init__(self):
        self.executor   = DatabricksExecutor()
        self.codegen    = PySparkCodeGenerator()
        self.summarizer = ResultSummarizer()
        self.classifier = QueryClassifier()
        self.adhoc      = AdhocLayer(self.executor, self.codegen, self.summarizer)
        self.active_file: dict | None = None

    # =========================================================================
    # FILE INGESTION
    # =========================================================================
    def attach_file(self, file_id: str, file_path: str, file_format: str) -> dict:
        """Run dataset registration: ingestion + profiling + metadata upload."""
        logger.info("Starting ingestion for: %s", file_path)
        format_bucket = _detect_format_bucket(file_format)

        ingestion = self.executor.ingest_and_profile(
            file_id=file_id, file_path=file_path, file_format=file_format
        )
        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema = ingestion["schema"]
        profiling = ingestion["profiling"]

        def _upload_metadata():
            uploader = MetadataUploader()
            uploader.upload(schema, file_id)
            logger.info("[L1] Schema uploaded to S3")

        def _upload_profiling():
            import boto3
            s3 = boto3.client("s3", region_name=settings.aws_region)
            key = f"profile/{file_id}/profile.json"
            s3.put_object(
                Bucket=settings.s3_bucket, Key=key,
                Body=json.dumps(profiling, indent=2), ContentType="application/json"
            )
            logger.info("[L2] Profile stats uploaded to S3")

        def _trigger_bedrock():
            try:
                trigger_bedrock_ingestion(bucket=settings.s3_bucket, prefix="schema/")
                logger.info("[KB] Bedrock ingestion triggered")
            except Exception as e:
                logger.warning("[KB] Bedrock ingestion failed: %s", e)

        with ThreadPoolExecutor(max_workers=3) as pool:
            pool.submit(_upload_metadata)
            pool.submit(_upload_profiling)
            pool.submit(_trigger_bedrock)

        self.active_file = {
            "file_id":      file_id,
            "file_path":    file_path,
            "format":       file_format,
            "format_bucket": format_bucket,
            "columns":      schema["columns"],
            "schema":       schema,
            "profiling":    profiling,
        }
        return profiling

    def load_from_s3(self, dataset_id: str, file_path: str, file_format: str):
        """Reconstruct active_file context from S3 for testing."""
        metadata = load_metadata_from_s3(dataset_id)
        profiling = load_profile_from_s3(dataset_id) or {}
        if not metadata:
            raise FileNotFoundError(f"Metadata not found in S3 for {dataset_id}")
            
        self.active_file = {
            "file_id":      dataset_id,
            "file_path":    file_path,
            "format":       file_format,
            "format_bucket": _detect_format_bucket(file_format),
            "columns":      metadata.get("columns", []),
            "schema":       metadata,
            "profiling":    profiling,
        }

    # =========================================================================
    # QUERY EXECUTION — Route to modular layers
    # =========================================================================
    def run(self, user_input: str, dataset_id: str) -> dict:
        """Route the user question through 4 modular layers."""
        if not self.active_file or self.active_file["file_id"] != dataset_id:
            raise RuntimeError("Dataset not loaded.")

        format_bucket = self.active_file.get("format_bucket", STRUCTURED)
        classification = self.classifier.classify(user_input)
        route = classification.route

        logger.info("Routing query [%s] | format=%s", route, format_bucket)

        # ------------------------------------------------------------------
        # Route 1: METADATA
        # ------------------------------------------------------------------
        if route == QueryRoute.METADATA:
            res = handle_metadata(user_input, dataset_id, format_bucket, self.active_file)
            if res: return res

        # ------------------------------------------------------------------
        # Route 2: PROFILE
        # ------------------------------------------------------------------
        if route == QueryRoute.PROFILE:
            res = handle_profile(user_input, dataset_id, format_bucket, self.active_file)
            if res: return res

        # ------------------------------------------------------------------
        # Route 3: KPI
        # ------------------------------------------------------------------
        if route == QueryRoute.KPI:
            return handle_kpi(user_input, dataset_id, format_bucket, self._handle_adhoc_proxy)

        # ------------------------------------------------------------------
        # Route 4: ADHOC (Fallback)
        # ------------------------------------------------------------------
        return self._handle_adhoc_proxy(user_input, dataset_id, format_bucket)

    def _handle_adhoc_proxy(self, user_input: str, dataset_id: str, format_bucket: str) -> dict:
        """Proxy to AdhocLayer to avoid circular dependency."""
        return self.adhoc.handle(user_input, dataset_id, format_bucket, self.active_file)