# Backend/core/query_orchestrator.py
#
# Redesigned Orchestrator — 4-Layer Architecture
#
# attach_file() pipeline (parallel):
#   ├── Layer 1: schema metadata → S3 + Bedrock KB
#   ├── Layer 2: column profiling → S3 profile store
#   └── Layer 3: KPI gold table generation → S3 semantic store
#
# run() query pipeline:
#   ├── Format detection (structured / semi-structured / unstructured)
#   ├── Query classification (METADATA / PROFILE / KPI / ADHOC)
#   └── Format-aware routing:
#         METADATA  → metadata_store answers from S3 schema
#         PROFILE   → profile_store answers from pre-computed stats
#         KPI       → semantic_layer answers from gold tables
#         ADHOC     → on-demand PySpark generation + Databricks execution

import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from query_generation.pyspark_generator import PySparkCodeGenerator, IrrelevantQueryError
from summarization.result_summarizer import ResultSummarizer
from classifier.query_classifier import QueryClassifier
from classifier.routing_schema import QueryRoute
from llm.llm_query import invoke_llm

logger = get_logger(__name__)

MAX_RETRIES = 3

# ---------------------------------------------------------------------------
# Format bucket constants
# ---------------------------------------------------------------------------
STRUCTURED       = "structured"        # CSV, Parquet, Delta, XLSX
SEMI_STRUCTURED  = "semi_structured"   # JSON, XML, Avro
UNSTRUCTURED     = "unstructured"      # PDF, DOCX, TXT, images

_FORMAT_MAP = {
    "csv":     STRUCTURED,
    "parquet": STRUCTURED,
    "delta":   STRUCTURED,
    "xlsx":    STRUCTURED,
    "xls":     STRUCTURED,
    "json":    SEMI_STRUCTURED,
    "xml":     SEMI_STRUCTURED,
    "avro":    SEMI_STRUCTURED,
    "pdf":     UNSTRUCTURED,
    "docx":    UNSTRUCTURED,
    "doc":     UNSTRUCTURED,
    "txt":     UNSTRUCTURED,
}


def _detect_format_bucket(file_format: str) -> str:
    """Map a file extension to a format bucket (structured/semi/unstructured)."""
    return _FORMAT_MAP.get(file_format.lower(), STRUCTURED)


# ---------------------------------------------------------------------------
# Profile S3 uploader  (Layer 2 — stored alongside schema)
# ---------------------------------------------------------------------------
def _upload_profile_to_s3(profiling: dict, file_id: str):
    """Upload pre-computed profiling stats to S3 profile store."""
    import boto3
    from pathlib import Path

    s3 = boto3.client("s3", region_name=settings.aws_region)
    key = f"profile/{file_id}/profile.json"

    logger.info("Uploading profile to s3://%s/%s", settings.s3_bucket, key)

    s3.put_object(
        Bucket=settings.s3_bucket,
        Key=key,
        Body=json.dumps(profiling, indent=2),
        ContentType="application/json"
    )

    logger.info("Profile upload complete: %s", key)
    return key


# ---------------------------------------------------------------------------
# Profile S3 reader  (Layer 2 — read at query time)
# ---------------------------------------------------------------------------
def _load_profile_from_s3(file_id: str) -> dict | None:
    """Load pre-computed profile stats from S3."""
    import boto3
    from botocore.exceptions import ClientError

    s3 = boto3.client("s3", region_name=settings.aws_region)
    key = f"profile/{file_id}/profile.json"

    try:
        resp = s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return json.loads(resp["Body"].read())
    except ClientError:
        logger.warning("Profile not found in S3 for %s", file_id)
        return None


# ---------------------------------------------------------------------------
# Metadata S3 reader  (Layer 1 — read at query time)
# ---------------------------------------------------------------------------
def _load_metadata_from_s3(file_id: str) -> dict | None:
    """Load schema metadata from S3."""
    import boto3
    from botocore.exceptions import ClientError
    from pathlib import Path

    s3 = boto3.client("s3", region_name=settings.aws_region)
    # uploader writes to schema/{stem}.json — we stored using file_id
    key = f"schema/{file_id}.json"

    try:
        resp = s3.get_object(Bucket=settings.s3_bucket, Key=key)
        return json.loads(resp["Body"].read())
    except ClientError:
        logger.warning("Metadata not found in S3 for %s", file_id)
        return None


class QueryOrchestrator:

    def __init__(self):
        self.executor   = DatabricksExecutor()
        self.codegen    = PySparkCodeGenerator()
        self.summarizer = ResultSummarizer()
        self.classifier = QueryClassifier()
        self.active_file: dict | None = None

    # =========================================================================
    # FILE INGESTION  — parallel: metadata + profiling + KPI build
    # =========================================================================
    def attach_file(self, file_id: str, file_path: str, file_format: str) -> dict:
        """
        Run dataset registration:
          1. Databricks ingest_and_profile job  (blocking — produces schema + profiling)
          2. In parallel after that:
               a. Upload schema (Layer 1) → S3 + trigger Bedrock KB
               b. Upload profiling stats (Layer 2) → S3 profile store
               c. [ Layer 3 KPI build deferred — requires separate Databricks job ]
        """

        logger.info("Starting ingestion for: %s", file_path)
        format_bucket = _detect_format_bucket(file_format)
        logger.info("Format bucket: %s", format_bucket)

        # -----------------------------------------------------------------------
        # Step 1 — Databricks: ingest + profile  (blocking, produces both outputs)
        # -----------------------------------------------------------------------
        ingestion = self.executor.ingest_and_profile(
            file_id=file_id,
            file_path=file_path,
            file_format=file_format
        )

        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema   = ingestion["schema"]
        profiling = ingestion["profiling"]

        # -----------------------------------------------------------------------
        # Step 2 — Parallel: upload metadata (L1) + profiling (L2) + Bedrock
        # -----------------------------------------------------------------------
        def _upload_metadata():
            uploader = MetadataUploader()
            uploader.upload(schema, file_id)
            logger.info("[L1] Schema uploaded to S3")

        def _upload_profiling():
            _upload_profile_to_s3(profiling, file_id)
            logger.info("[L2] Profile stats uploaded to S3")

        def _trigger_bedrock():
            try:
                result = trigger_bedrock_ingestion(
                    bucket=settings.s3_bucket,
                    prefix="schema/"
                )
                logger.info("[KB] Bedrock ingestion triggered: %s", result)
            except Exception as e:
                logger.warning("[KB] Bedrock ingestion failed (non-fatal): %s", e)

        futures_map = {}
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures_map["metadata"]  = pool.submit(_upload_metadata)
            futures_map["profiling"] = pool.submit(_upload_profiling)
            futures_map["bedrock"]   = pool.submit(_trigger_bedrock)

            for name, fut in futures_map.items():
                try:
                    fut.result()
                except Exception as e:
                    logger.warning("Parallel task [%s] failed: %s", name, e)

        # -----------------------------------------------------------------------
        # Step 3 — Store active file context (in-memory)
        # -----------------------------------------------------------------------
        self.active_file = {
            "file_id":      file_id,
            "file_path":    file_path,
            "format":       file_format,
            "format_bucket": format_bucket,
            "columns":      schema["columns"],
            "schema":       schema,
            "profiling":    profiling,
        }

        logger.info("Ingestion complete for %s", file_id)
        return profiling

    # =========================================================================
    # QUERY EXECUTION — classified + format-aware routing
    # =========================================================================
    def run(self, user_input: str, dataset_id: str) -> dict:
        """
        Route the user question through 4 layers:
          METADATA → answer from stored schema (no PySpark)
          PROFILE  → answer from pre-computed stats (no PySpark)
          KPI      → answer from gold tables (no PySpark)  [Phase C]
          ADHOC    → generate PySpark + execute on Databricks
        """

        if not self.active_file or self.active_file["file_id"] != dataset_id:
            raise RuntimeError("Dataset not loaded. Please upload a file first.")

        format_bucket = self.active_file.get("format_bucket", STRUCTURED)

        # ------------------------------------------------------------------
        # Step 1 — Classify the question
        # ------------------------------------------------------------------
        classification = self.classifier.classify(user_input)
        route = classification.route

        logger.info(
            "Question classified as [%s] (confidence=%.2f) | format=%s",
            route, classification.confidence, format_bucket
        )

        # ------------------------------------------------------------------
        # Step 2 — Route: METADATA
        # ------------------------------------------------------------------
        if route == QueryRoute.METADATA:
            return self._handle_metadata(user_input, dataset_id, format_bucket)

        # ------------------------------------------------------------------
        # Step 3 — Route: PROFILE
        # ------------------------------------------------------------------
        if route == QueryRoute.PROFILE:
            return self._handle_profile(user_input, dataset_id, format_bucket)

        # ------------------------------------------------------------------
        # Step 4 — Route: KPI  (Phase C — gold tables not yet built)
        # ------------------------------------------------------------------
        if route == QueryRoute.KPI:
            return self._handle_kpi(user_input, dataset_id, format_bucket)

        # ------------------------------------------------------------------
        # Step 5 — Route: ADHOC → PySpark generation + execution
        # ------------------------------------------------------------------
        return self._handle_adhoc(user_input, dataset_id, format_bucket)

    # =========================================================================
    # LAYER 1 — METADATA HANDLER
    # =========================================================================

    import re

    # ---------------------------------------------------------------------------
    # Heuristics for dimension / measure classification
    # ---------------------------------------------------------------------------

    # Data types that are almost always measures (numeric, aggregatable)
    _MEASURE_DTYPES = {
        "int", "integer", "bigint", "smallint", "tinyint",
        "float", "double", "decimal", "numeric", "real",
        "long", "short", "number",
    }

    # Data types that are almost always dimensions (categorical, descriptive)
    _DIMENSION_DTYPES = {
        "string", "varchar", "char", "text", "nvarchar",
        "boolean", "bool",
        "date", "timestamp", "datetime",
    }

    # Column name patterns that strongly suggest a MEASURE role
    _MEASURE_NAME_PATTERNS = [
        r"\bpay\b", r"\bsalary\b", r"\bwage\b", r"\bcost\b", r"\bprice\b",
        r"\bamount\b", r"\brevenue\b", r"\bprofit\b", r"\bloss\b",
        r"\bcount\b", r"\bquantity\b", r"\bqty\b", r"\bunits\b",
        r"\bage\b", r"\btenure\b", r"\bscore\b", r"\brating\b",
        r"\brate\b", r"\bpercent\b", r"\bpct\b", r"\bratio\b",
        r"\byear\b", r"\bmonth\b", r"\bday\b",         # numeric time fields
        r"\bduration\b", r"\blatitude\b", r"\blongitude\b",
    ]

    # Column name patterns that strongly suggest a DIMENSION role
    _DIMENSION_NAME_PATTERNS = [
        r"\bname\b", r"\bcode\b", r"\bid\b", r"\bkey\b", r"\btype\b",
        r"\bcategory\b", r"\bstatus\b", r"\bflag\b", r"\blabel\b",
        r"\bgender\b", r"\bdepartment\b", r"\bdesignation\b", r"\brole\b",
        r"\blocation\b", r"\bcity\b", r"\bstate\b", r"\bcountry\b",
        r"\bregion\b", r"\bzone\b", r"\bteam\b", r"\bdivision\b",
        r"\bqualification\b", r"\bcourse\b", r"\buniversity\b",
        r"\bemployment\b", r"\bcompany\b", r"\btitle\b",
    ]

    # Columns that are identifiers — neither dimension nor measure
    _IDENTIFIER_NAME_PATTERNS = [
        r"^id$", r"_id$", r"code$", r"^emp", r"^cust", r"^order",
        r"^uuid", r"^guid", r"^pk", r"^fk",
    ]

    # Columns that are timestamps
    _TIMESTAMP_NAME_PATTERNS = [
        r"\bdate\b", r"\bdoj\b", r"\bcreated\b", r"\bupdated\b",
        r"\btimestamp\b", r"\btime\b", r"\bjoined\b", r"\bstart\b", r"\bend\b",
    ]


    def _classify_column(name: str, dtype: str) -> str:
        """
        Classify a single column as: IDENTIFIER | TIMESTAMP | DIMENSION | MEASURE | UNKNOWN.

        Priority order:
            1. Identifier patterns  (name-based)
            2. Timestamp patterns   (name-based + dtype)
            3. Dtype-based          (reliable for typed sources like Parquet/Delta)
            4. Name-based patterns  (fallback for loosely typed sources like CSV)
            5. UNKNOWN              (hand off to LLM for final call)
        """
        n = name.lower().strip()
        d = dtype.lower().strip()

        # 1. Identifier
        if any(re.search(p, n) for p in _IDENTIFIER_NAME_PATTERNS):
            return "IDENTIFIER"

        # 2. Timestamp — dtype or name
        if d in ("date", "timestamp", "datetime") or any(re.search(p, n) for p in _TIMESTAMP_NAME_PATTERNS):
            return "TIMESTAMP"

        # 3. Dtype-based (most reliable)
        if d in _MEASURE_DTYPES:
            return "MEASURE"
        if d in _DIMENSION_DTYPES:
            # Further disambiguate — boolean and string can still be timestamps
            return "DIMENSION"

        # 4. Name-based fallback (for string-typed numeric columns, e.g. "10LPA" in CSV)
        if any(re.search(p, n) for p in _MEASURE_NAME_PATTERNS):
            return "MEASURE"
        if any(re.search(p, n) for p in _DIMENSION_NAME_PATTERNS):
            return "DIMENSION"

        # 5. Cannot determine — LLM will handle in the prompt
        return "UNKNOWN"


    def _build_column_classification(columns: list) -> dict:
        """
        Returns a dict with four buckets: identifiers, timestamps, dimensions, measures.
        Each bucket contains a list of {name, type, nullable} dicts.
        Also returns a list of UNKNOWN columns for LLM resolution.
        """
        buckets = {
            "identifiers": [],
            "timestamps":  [],
            "dimensions":  [],
            "measures":    [],
            "unknown":     [],
        }
        for col in columns:
            name     = col.get("name", col.get("column_name", "?"))
            dtype    = col.get("type", col.get("data_type", "string"))
            nullable = col.get("nullable", True)
            role     = _classify_column(name, dtype)

            entry = {"name": name, "type": dtype, "nullable": nullable}
            bucket_key = {
                "IDENTIFIER": "identifiers",
                "TIMESTAMP":  "timestamps",
                "DIMENSION":  "dimensions",
                "MEASURE":    "measures",
                "UNKNOWN":    "unknown",
            }[role]
            buckets[bucket_key].append(entry)

        return buckets


    def _format_bucket_summary(buckets: dict) -> str:
        """Build a readable schema summary with role annotations."""
        lines = []

        role_labels = [
            ("identifiers", "🔑 Identifiers"),
            ("timestamps",  "📅 Timestamps"),
            ("dimensions",  "📂 Dimensions"),
            ("measures",    "📊 Measures"),
            ("unknown",     "❓ Unclassified (review needed)"),
        ]

        for key, label in role_labels:
            cols = buckets.get(key, [])
            if not cols:
                continue
            lines.append(f"\n{label} ({len(cols)}):")
            for c in cols:
                nullable_tag = "nullable" if c["nullable"] else "not null"
                lines.append(f"  • {c['name']}  [{c['type']}]  ({nullable_tag})")

        return "\n".join(lines)


    def _handle_metadata(self, question: str, dataset_id: str, format_bucket: str) -> dict:
        """
        Answer schema/structural questions from stored metadata — no PySpark.
        Now includes dimension/measure/identifier/timestamp classification.
        """

        # ------------------------------------------------------------------
        # Unstructured: schema concepts don't apply
        # ------------------------------------------------------------------
        if format_bucket == UNSTRUCTURED:
            return {
                "route":         "METADATA",
                "format_bucket": format_bucket,
                "answer": (
                    "This is an unstructured document. "
                    "Schema-level questions are not applicable. "
                    "Try asking 'what is this document about?' instead."
                ),
                "pyspark":  None,
                "results":  None,
            }

        # ------------------------------------------------------------------
        # Load metadata from S3 or in-memory fallback
        # ------------------------------------------------------------------
        metadata = (
            _load_metadata_from_s3(dataset_id)
            or self.active_file.get("schema", {})
        )
        if not metadata:
            logger.warning("[L1] No metadata found — falling back to ADHOC")
            return self._handle_adhoc(question, dataset_id, format_bucket)

        columns   = metadata.get("columns", [])
        file_fmt  = metadata.get("format", self.active_file.get("format", "unknown"))
        file_path = metadata.get("data_location", {}).get("path", "")

        # ------------------------------------------------------------------
        # Classify every column into role buckets
        # ------------------------------------------------------------------
        buckets        = _build_column_classification(columns)
        schema_summary = _format_bucket_summary(buckets)

        # ------------------------------------------------------------------
        # Structured schema header
        # ------------------------------------------------------------------
        header = (
            f"Dataset : {file_path}\n"
            f"Format  : {file_fmt.upper()}\n"
            f"Columns : {len(columns)} total  |  "
            f"{len(buckets['identifiers'])} identifiers  |  "
            f"{len(buckets['timestamps'])} timestamps  |  "
            f"{len(buckets['dimensions'])} dimensions  |  "
            f"{len(buckets['measures'])} measures  |  "
            f"{len(buckets['unknown'])} unclassified"
        )

        raw_answer = f"{header}\n{schema_summary}"

        # ------------------------------------------------------------------
        # Warn if many columns are unclassified
        # (common for CSV with messy string types — LLM will resolve below)
        # ------------------------------------------------------------------
        unknown_note = ""
        if buckets["unknown"]:
            unknown_cols = ", ".join(c["name"] for c in buckets["unknown"])
            unknown_note = (
                f"\n\nNote: {len(buckets['unknown'])} column(s) could not be "
                f"auto-classified: [{unknown_cols}]. "
                f"The LLM will attempt to resolve these from context."
            )

        # ------------------------------------------------------------------
        # LLM pass — answers the specific question + resolves unknowns
        # ------------------------------------------------------------------
        try:
            unknown_block = ""
            if buckets["unknown"]:
                unknown_list = "\n".join(
                    f"  - {c['name']} [{c['type']}]" for c in buckets["unknown"]
                )
                unknown_block = (
                    f"\nThe following columns could not be auto-classified. "
                    f"Please classify each as DIMENSION or MEASURE based on name and context:\n"
                    f"{unknown_list}\n"
                )

            llm_prompt = (
                f"The user asked: \"{question}\"\n\n"
                f"Here is the dataset schema with column role classifications:\n"
                f"{raw_answer}\n"
                f"{unknown_block}\n"
                f"Instructions:\n"
                f"1. Answer the user's question using only the schema information above.\n"
                f"2. If the question is about dimensions or measures, refer to the classified buckets.\n"
                f"3. If there are unclassified columns, suggest their likely role (DIMENSION/MEASURE) "
                f"   with a brief reason.\n"
                f"4. Be concise and direct. Do not invent column names or data not shown above."
            )
            final_answer = invoke_llm(prompt=llm_prompt, temperature=0.1, max_tokens=600)
        except Exception as e:
            logger.warning("[L1] LLM formatting failed, using raw schema: %s", e)
            final_answer = raw_answer + unknown_note

        # ------------------------------------------------------------------
        # Return — include buckets in results for downstream use
        # ------------------------------------------------------------------
        return {
            "route":         "METADATA",
            "format_bucket": format_bucket,
            "answer":        final_answer,
            "pyspark":       None,
            "results": {
                "column_classification": buckets,
                "total_columns":         len(columns),
                "path":                  file_path,
                "format":                file_fmt,
            },
        }

    # =========================================================================
    # LAYER 2 — PROFILE HANDLER
    # =========================================================================
    def _handle_profile(self, question: str, dataset_id: str, format_bucket: str) -> dict:
        """Answer distribution/quality questions from pre-computed stats — no PySpark."""

        if format_bucket == UNSTRUCTURED:
            return {
                "route": "PROFILE",
                "format_bucket": format_bucket,
                "answer": (
                    "Statistical profiling is not meaningful for unstructured documents "
                    "(PDF, DOCX, TXT). Try asking a metadata or content question instead."
                ),
                "pyspark": None,
                "results": None,
            }

        # Load profiling from S3 (or fall back to in-memory stats from attach_file)
        profile = (
            _load_profile_from_s3(dataset_id)
            or self.active_file.get("profiling", {})
        )

        if not profile:
            logger.warning("[L2] No profile found — falling back to ADHOC")
            return self._handle_adhoc(question, dataset_id, format_bucket)

        # Build a structured stats summary
        total_rows = profile.get("row_count", "?")
        total_cols = profile.get("column_count", "?")
        cols       = profile.get("columns", [])

        col_stats = []
        for col in cols:
            name       = col.get("column_name", col.get("name", "?"))
            dtype      = col.get("data_type", "?")
            null_pct   = col.get("null_percentage", col.get("null_pct", 0))
            distinct   = col.get("distinct_count", "?")
            min_v      = col.get("min", None)
            max_v      = col.get("max", None)
            mean_v     = col.get("mean", None)
            top_vals   = col.get("top_values", col.get("sample_values", []))

            parts = [f"  • {name} [{dtype}] | nulls={null_pct}% | distinct={distinct}"]
            if min_v is not None and max_v is not None:
                parts.append(f"    range: {min_v} → {max_v}")
            if mean_v is not None:
                parts.append(f"    mean:  {mean_v}")
            if top_vals:
                if isinstance(top_vals[0], dict):
                    vals_str = ", ".join(
                        f"{v.get('value', '?')} ({v.get('count', '?')})"
                        for v in top_vals[:5]
                    )
                else:
                    vals_str = ", ".join(str(v) for v in top_vals[:5] if v is not None)
                parts.append(f"    top values: {vals_str}")

            col_stats.append("\n".join(parts))

        stats_text = "\n".join(col_stats)
        summary_text = (
            f"Total rows: {total_rows} | Total columns: {total_cols}\n\n"
            f"{stats_text}"
        )

        # Use LLM to pick out the exact answer to the user's question
        try:
            llm_prompt = (
                f"The user asked: \"{question}\"\n\n"
                f"Here are the pre-computed column statistics:\n{summary_text}\n\n"
                "Answer the user's specific question using only the statistics above. "
                "Be direct and precise. Include numbers."
            )
            answer = invoke_llm(prompt=llm_prompt, temperature=0.1, max_tokens=400)
        except Exception as e:
            logger.warning("[L2] LLM formatting failed, using raw stats: %s", e)
            answer = summary_text

        return {
            "route":        "PROFILE",
            "format_bucket": format_bucket,
            "answer":       answer,
            "pyspark":      None,
            "results":      None,
        }

    # =========================================================================
    # LAYER 3 — KPI HANDLER  (Phase C placeholder)
    # =========================================================================
    def _handle_kpi(self, question: str, dataset_id: str, format_bucket: str) -> dict:
        """
        Answer KPI/business-metric questions from pre-aggregated gold tables.
        Phase C: gold tables not yet built → falls back to ADHOC PySpark.
        """

        if format_bucket == UNSTRUCTURED:
            return {
                "route": "KPI",
                "format_bucket": format_bucket,
                "answer": (
                    "KPI metrics are not available for unstructured documents. "
                    "Try a structured CSV or Parquet file."
                ),
                "pyspark": None,
                "results": None,
            }

        # Phase C TODO: load from semantic_layer.kpi_reader
        # For now, fall through to ADHOC so the question still gets answered
        logger.info("[L3] KPI gold tables not yet built — routing to ADHOC PySpark")
        result = self._handle_adhoc(question, dataset_id, format_bucket)
        result["route"] = "KPI→ADHOC"   # signal to frontend that we fell back
        return result

    # =========================================================================
    # LAYER 4 — ADHOC PYSPARK HANDLER
    # =========================================================================
    def _handle_adhoc(self, user_input: str, dataset_id: str, format_bucket: str) -> dict:
        """
        Generate PySpark on demand and execute on Databricks.
        Includes relevance guard, code generation, and retry+correction loop.
        """

        context = {
            "file_path": self.active_file["file_path"],
            "format":    self.active_file["format"],
            "columns":   self.active_file["columns"],
        }

        # ------------------------------------------------------------------
        # Relevance guard + initial code generation
        # ------------------------------------------------------------------
        try:
            pyspark_code = self.codegen.generate(user_input, context)
        except IrrelevantQueryError as e:
            logger.info("Irrelevant query blocked: %s", e)
            return {
                "route":       "IRRELEVANT",
                "user_input":  user_input,
                "irrelevant":  True,
                "message": (
                    "This question does not appear to relate to the loaded dataset. "
                    "Please ask something about the data — its structure, quality, "
                    "trends, distributions, or business metrics."
                ),
                "pyspark": None,
                "results": None,
                "insights": None,
                "text_results": None,
            }

        # ------------------------------------------------------------------
        # Execute with retry + LLM correction loop
        # ------------------------------------------------------------------
        last_error = None
        last_code  = pyspark_code

        for attempt in range(1, MAX_RETRIES + 1):
            logger.info("Execution attempt %d / %d", attempt, MAX_RETRIES)

            execution = self.executor.execute_query(context, last_code)

            if execution["status"] == "SUCCESS":
                result = execution["result"]

                with ThreadPoolExecutor(max_workers=2) as tpool:
                    future_summary = tpool.submit(
                        self.summarizer.summarize, user_input, result, "query"
                    )
                    future_explain = tpool.submit(
                        self.summarizer.explain, user_input, result
                    )
                    summary      = future_summary.result()
                    text_results = future_explain.result()

                return {
                    "route":        "ADHOC",
                    "format_bucket": format_bucket,
                    "user_input":   user_input,
                    "pyspark":      last_code,
                    "results":      result,
                    "insights":     summary,
                    "text_results": text_results,
                    "attempts":     attempt,
                }

            last_error = execution.get("error", "Unknown execution error")
            logger.warning("Execution attempt %d failed: %s", attempt, last_error)

            if attempt < MAX_RETRIES:
                try:
                    corrected_code = self.codegen.correct(
                        question=user_input,
                        context=context,
                        failing_code=last_code,
                        error_message=last_error
                    )
                    last_code = corrected_code
                except RuntimeError as e:
                    logger.warning("LLM correction failed on attempt %d: %s", attempt, e)
                    break

        # All retries exhausted
        logger.error("All %d attempts exhausted. Last error: %s", MAX_RETRIES, last_error)

        return {
            "route":        "ADHOC",
            "format_bucket": format_bucket,
            "user_input":   user_input,
            "error":        last_error,
            "pyspark":      last_code,
            "results":      None,
            "insights":     None,
            "text_results": None,
            "attempts":     MAX_RETRIES,
        }