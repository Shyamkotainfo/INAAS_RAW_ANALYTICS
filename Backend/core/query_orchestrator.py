# Backend/core/query_orchestrator.py

<<<<<<< HEAD
import json
import re
from typing import Any
=======
import hashlib
>>>>>>> f3d12b2 (context doc update)

from logger.logger import get_logger
from execution.databricks_executor import DatabricksExecutor
from ingestion.metadata_uploader import MetadataUploader
from rag.bedrock_ingestor import trigger_bedrock_ingestion
from config.settings import settings
from prompt.pyspark_code_gen import build_semantic_context
from query_generation.pyspark_generator import (
    PySparkCodeGenerator,
    IrrelevantQueryError,
    load_business_rule,
    load_guardrails,
    resolve_columns,
)
from summarization.result_summarizer import ResultSummarizer

logger = get_logger(__name__)

MAX_RETRIES = 3


class QueryOrchestrator:

    def __init__(self):
        self.executor = DatabricksExecutor()
        self.codegen = PySparkCodeGenerator()
        self.summarizer = ResultSummarizer()
        self.active_file = None

    # =====================================================
    # FILE INGESTION
    # =====================================================
    def attach_file(self, file_id: str, file_path: str, file_format: str, context: str = None):

        logger.info("Starting ingestion for file: %s", file_path)

        # ----------------------------
        # Databricks profiling
        # ----------------------------
        ingestion = self.executor.ingest_and_profile(
            file_id=file_id,
            file_path=file_path,
            file_format=file_format
        )

        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema = ingestion["schema"]
        profiling = ingestion["profiling"]

        if context:
            schema["semantic_context"] = context
        semantic_context = None
        semantic_context_hash = None
        if context:
            logger.info("Wiki root configured for targeted retrieval: %s", context)
            schema["semantic_context_path"] = context

        # ----------------------------
        # Upload schema to S3
        # ----------------------------
        uploader = MetadataUploader()
        uploader.upload(schema, file_id)

        # ----------------------------
        # Trigger Bedrock ingestion (SAFE — non-blocking)
        # ----------------------------
        try:
            ingestion_status = trigger_bedrock_ingestion(
                bucket=settings.s3_bucket,
                prefix="schema/"
            )
            logger.info("Bedrock ingestion trigger response: %s", ingestion_status)
        except Exception as e:
            logger.warning(
                "Bedrock ingestion failed but profiling succeeded: %s", str(e)
            )

        logger.info("Schema uploaded to S3 and ingestion process handled")

        # ----------------------------
        # Store active file in memory
        # ----------------------------
        self.active_file = {
            "file_id": file_id,
            "file_path": file_path,
            "format": file_format,
            "columns": schema["columns"],
            "semantic_context": schema.get("semantic_context")
            "semantic_context": semantic_context,
            "semantic_context_path": context,
            "semantic_context_hash": semantic_context_hash
        }

        return profiling

    # =====================================================
    # QUERY EXECUTION  (with retry + correction loop)
    # =====================================================
    def run(self, user_input: str, dataset_id: str) -> dict:

        if not self.active_file or self.active_file["file_id"] != dataset_id:
            raise RuntimeError("Dataset not loaded. Please run profiling first.")

        context = {
            "file_path": self.active_file["file_path"],
            "format": self.active_file["format"],
            "columns": self.active_file["columns"],
            "semantic_context": self.active_file.get("semantic_context"),
            "semantic_context_path": self.active_file.get("semantic_context_path"),
        }
        context["resolved_terms"] = self._resolve_schema_terms(context)
        semantic_column_error = self._detect_unavailable_semantic_column(user_input, context)
        if semantic_column_error:
            return {
                "user_input": user_input,
                "error": semantic_column_error,
                "query": None,
                "pyspark": None,
                "results": None,
                "insights": None,
                "text_results": semantic_column_error,
                "analysis_note": self._build_analysis_note(user_input, context),
                "attempts": 0,
            }
        analysis_note = self._build_analysis_note(user_input, context)

        columns = ", ".join(c["name"] for c in context["columns"])
        context["resolved_mappings"] = resolve_columns(
            available_columns=context["columns"],
            domain="hr"
        )
        context["business_rule"] = load_business_rule(
            question=user_input,
            domain="hr"
        )
        context["guardrails"] = load_guardrails(domain="hr")
        context["semantic_context"] = build_semantic_context(
            question=user_input,
            columns=columns,
            top_k=1
        )
        self.active_file["semantic_context"] = context["semantic_context"]
        self.active_file["semantic_context_hash"] = self._hash_text(context["semantic_context"])

        context_debug = self._build_context_debug(context)
        logger.info(
            "Passing semantic context to LLM | used=%s | path=%s | chars=%d | hash=%s",
            context_debug["context_used"],
            context_debug["context_path"],
            context_debug["context_chars"],
            context_debug["context_hash"]
        )
        logger.info("QUESTION=%s", user_input)
        logger.info("AVAILABLE_COLUMNS=%s", columns)
        logger.info("RESOLVED_MAPPINGS=%s", context["resolved_mappings"])
        logger.info("SELECTED_BUSINESS_RULE=%s", context["business_rule"])

        # --------------------------------------------------
        # Step 1 — Relevance guard + initial code generation
        # --------------------------------------------------
        try:
            pyspark_code = self.codegen.generate(user_input, context)
        except IrrelevantQueryError as e:
            logger.info("Irrelevant query blocked: %s", str(e))
            return {
                "user_input": user_input,
                "irrelevant": True,
                "message": (
                    "This question does not appear to relate to the loaded dataset. "
                    "Please ask something about the data — its structure, quality, "
                    "trends, distributions, or business metrics."
                ),
                "pyspark": None,
                "results": None,
                "insights": None,
                "text_results": None,
                "context_debug": context_debug,
            }

        # --------------------------------------------------
        # Step 2 — Execute with up to MAX_RETRIES correction attempts
        # --------------------------------------------------
        last_error = None
        last_code = pyspark_code

        for attempt in range(1, MAX_RETRIES + 1):
            logger.info("Execution attempt %d / %d", attempt, MAX_RETRIES)

            execution = self.executor.execute_query(context, last_code)

            if execution["status"] == "SUCCESS":
                result = execution["result"]

                # Run both LLM insights and explanation in parallel to minimize latency
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=2) as tpool:
                    future_summary = tpool.submit(self.summarizer.summarize, user_input, result, "query")
                    future_explain = tpool.submit(self.summarizer.explain, user_input, result)
                    
                    summary = future_summary.result()
                    text_results = future_explain.result()

                return {
                    "user_input": user_input,
                    "pyspark": last_code,
                    "results": result,
                    "insights": summary,
                    "text_results": text_results,
                    "analysis_note": analysis_note,
                    "attempts": attempt,
                    "context_debug": context_debug,
                }

            # FAILED — capture error and try to correct
            last_error = execution.get("error", "Unknown execution error")

            logger.warning(
                "Execution attempt %d failed: %s", attempt, last_error
            )

            if attempt < MAX_RETRIES:
                logger.info("Requesting LLM correction for attempt %d", attempt + 1)
                try:
                    corrected_code = self.codegen.correct(
                        question=user_input,
                        context=context,
                        failing_code=last_code,
                        error_message=last_error
                    )
                    last_code = corrected_code
                except RuntimeError as e:
                    logger.warning(
                        "LLM correction produced invalid code on attempt %d: %s",
                        attempt, str(e)
                    )
                    # Keep last_code unchanged — try again with same code is pointless,
                    # but we break to avoid infinite correction loops on structural failures
                    break

        # --------------------------------------------------
        # Step 3 — All retries exhausted
        # --------------------------------------------------
        logger.error(
            "All %d execution attempts exhausted. Last error: %s", MAX_RETRIES, last_error
        )

        return {
            "user_input": user_input,
            "error": last_error,
            "query": last_code,
            "pyspark": last_code,
            "results": None,
            "insights": None,
            "text_results": None,
            "analysis_note": analysis_note,
            "attempts": MAX_RETRIES,
            "context_debug": context_debug,
        }

    def _build_analysis_note(self, user_input: str, context: dict) -> str | None:
        lowered = user_input.lower()
        mentions_comp = any(term in lowered for term in {"compensation", "pay", "salary"})
        mentions_band = "band" in lowered
        has_band_metadata = self._has_explicit_compensation_band_metadata(
            context.get("semantic_context")
        )

        if mentions_comp and mentions_band and not has_band_metadata:
            return (
                "No explicit compensation band metadata exists in the loaded semantic context. "
                "This analysis should default to designation-level average compensation. "
                "Min/max or percentile-style bands would require explicit band metadata or your confirmation."
            )

        return None

    def _detect_unavailable_semantic_column(self, user_input: str, context: dict) -> str | None:
        available = {col["name"].lower() for col in context.get("columns", [])}
        resolved_terms = context.get("resolved_terms", {})
        lowered = user_input.lower()

        concept_aliases = {
            "department": {"department", "dept", "dept_name"},
            "manager": {"manager", "manager_id", "manager_name"},
            "location": {"location", "city", "region", "office_location"},
            "business unit": {"business unit", "business_unit", "division"},
        }

        for concept, aliases in concept_aliases.items():
            if any(self._contains_term(lowered, alias) for alias in aliases):
                if concept in resolved_terms:
                    continue
                has_column = any(
                    alias in available
                    for alias in {item.replace(" ", "_") for item in aliases} | aliases
                )
                if not has_column:
                    return (
                        f"The loaded dataset does not contain a `{concept}` column, so this question "
                        f"cannot be answered directly from the uploaded schema. Ask using available columns "
                        f"such as Designation, Employment_Type, Gender, DOJ, company1/company2/company3, or Pay."
                    )

        return None

    def _resolve_schema_terms(self, context: dict) -> dict[str, str]:
        columns = [col["name"] for col in context.get("columns", [])]
        available = {col.lower(): col for col in columns}
        resolved: dict[str, str] = {}

        builtin_aliases = {
            "salary": ["Pay"],
            "pay": ["Pay"],
            "compensation": ["Pay"],
            "ctc": ["Pay"],
            "lpa": ["Pay"],
            "joining date": ["DOJ"],
            "date of joining": ["DOJ"],
            "joining": ["DOJ"],
            "hire date": ["DOJ"],
            "doj": ["DOJ"],
            "employment type": ["Employment_Type"],
            "employment": ["Employment_Type"],
            "employment category": ["Employment_Type"],
            "job title": ["Designation", "JobTitle1", "JobTitle2", "JobTitle3"],
            "role": ["Designation", "JobTitle1", "JobTitle2", "JobTitle3"],
            "job role": ["Designation", "JobTitle1", "JobTitle2", "JobTitle3"],
            "designation": ["Designation"],
            "gender": ["Gender"],
            "qualification": ["Qualification"],
            "course": ["Course"],
            "university": ["University"],
        }

        for concept, candidates in builtin_aliases.items():
            for candidate in candidates:
                if candidate.lower() in available:
                    resolved[concept] = available[candidate.lower()]
                    break

        semantic_context = context.get("semantic_context")
        if semantic_context:
            resolved.update(self._resolve_semantic_aliases(semantic_context, available))

        return resolved

    def _resolve_semantic_aliases(self, semantic_context: str, available: dict[str, str]) -> dict[str, str]:
        resolved: dict[str, str] = {}

        parsed = self._parse_semantic_context(semantic_context)
        if not isinstance(parsed, dict):
            return resolved

        alias_sources = []
        alias_sources.extend(parsed.get("dimensions", []))
        alias_sources.extend(parsed.get("measurable_fields", []))
        domain_summary = parsed.get("business_definitions", {})
        for items in domain_summary.values():
            if isinstance(items, list):
                alias_sources.extend(items)

        for item in alias_sources:
            if not isinstance(item, dict):
                continue
            concept = item.get("dimension") or item.get("field") or item.get("term")
            names = item.get("typical_column_names", [])
            if not concept or not isinstance(names, list):
                continue

            for name in names:
                normalized = str(name).lower().replace(" ", "_")
                if normalized in available:
                    resolved[concept.lower()] = available[normalized]
                    break

        return resolved

    def _parse_semantic_context(self, semantic_context: str | None) -> dict[str, Any] | None:
        if not semantic_context:
            return None

        try:
            parsed = json.loads(semantic_context)
        except Exception:
            return None

        return parsed if isinstance(parsed, dict) else None

    def _has_explicit_compensation_band_metadata(self, semantic_context: str | None) -> bool:
        parsed = self._parse_semantic_context(semantic_context)
        if not parsed:
            return False

        band_keys = {
            "bands",
            "band_definitions",
            "band_metadata",
            "compensation_bands",
            "salary_bands",
            "ranges",
            "range_definitions",
            "percentiles",
            "lower_bound",
            "upper_bound",
            "min_pay",
            "max_pay",
        }
        band_terms = {"compensation band", "salary band", "pay band", "band"}

        def walk(value: Any) -> bool:
            if isinstance(value, dict):
                lowered_keys = {str(key).lower() for key in value.keys()}
                if lowered_keys & band_keys:
                    return True

                descriptor = " ".join(
                    str(value.get(key, "")).lower()
                    for key in ("term", "field", "dimension", "name", "title")
                )
                if any(term in descriptor for term in band_terms):
                    if lowered_keys & {"bands", "ranges", "percentiles", "lower_bound", "upper_bound", "min", "max"}:
                        return True

                return any(walk(item) for item in value.values())

            if isinstance(value, list):
                return any(walk(item) for item in value)

            return False

        return walk(parsed)

    def _contains_term(self, text: str, term: str) -> bool:
        escaped = re.escape(term).replace("_", r"[_\s]+").replace(r"\ ", r"\s+")
        return bool(re.search(rf"(?<!\w){escaped}(?!\w)", text))
    def _build_context_debug(self, context: dict) -> dict:
        semantic_context = context.get("semantic_context") or ""
        return {
            "context_used": bool(semantic_context),
            "context_path": self.active_file.get("semantic_context_path") if self.active_file else None,
            "context_chars": len(semantic_context),
            "context_hash": self.active_file.get("semantic_context_hash") if self.active_file else None,
            "resolved_mappings": context.get("resolved_mappings") or {},
            "business_rule": context.get("business_rule"),
        }

    def _hash_text(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
