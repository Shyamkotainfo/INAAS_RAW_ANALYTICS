# Backend/core/query_orchestrator.py

import json
import re
from typing import Any
import hashlib
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
from llm.llm_query import LLMInvocationError

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
    def start_attach_file(
        self,
        file_id: str,
        file_path: str,
        file_format: str,
    ) -> str:
        logger.info("Submitting async ingestion for file: %s", file_path)
        return self.executor.submit_ingest_and_profile(
            file_id=file_id,
            file_path=file_path,
            file_format=file_format
        )

    def finalize_attach_file(
        self,
        file_id: str,
        file_path: str,
        file_format: str,
        context: str = None,
        domain: str = None,
        run_id: str = None
    ):
        logger.info("Finalizing ingestion for file: %s | run_id=%s", file_path, run_id)

        if run_id:
            ingestion = self.executor.finalize_ingest_and_profile(run_id)
        else:
            ingestion = self.executor.ingest_and_profile(
                file_id=file_id,
                file_path=file_path,
                file_format=file_format
            )

        return self._complete_ingestion(
            ingestion=ingestion,
            file_id=file_id,
            file_path=file_path,
            file_format=file_format,
            context=context,
            domain=domain
        )

    def attach_file(
        self,
        file_id: str,
        file_path: str,
        file_format: str,
        context: str = None,
        domain: str = None
    ):

        logger.info("Starting ingestion for file: %s", file_path)

        # ----------------------------
        # Databricks profiling
        # ----------------------------
        ingestion = self.executor.ingest_and_profile(
            file_id=file_id,
            file_path=file_path,
            file_format=file_format
        )

        return self._complete_ingestion(
            ingestion=ingestion,
            file_id=file_id,
            file_path=file_path,
            file_format=file_format,
            context=context,
            domain=domain
        )

        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema = ingestion["schema"]
        profiling = ingestion["profiling"]

        if context:
            schema["semantic_context"] = context
        semantic_context = None
        semantic_context_hash = None
        selected_domain = domain or self._resolve_domain_from_context(context)
        if context:
            logger.info("Wiki root configured for targeted retrieval: %s", context)
            schema["semantic_context_path"] = context
        if selected_domain:
            schema["business_context_domain"] = selected_domain

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
            "semantic_context": schema.get("semantic_context"),
            "semantic_context": semantic_context,
            "semantic_context_path": context,
            "semantic_context_hash": semantic_context_hash,
            "business_context_domain": selected_domain
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
            "business_context_domain": self.active_file.get("business_context_domain"),
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
        business_context_enabled = bool(context.get("semantic_context_path"))
        business_context_enabled = bool(
            context.get("semantic_context_path")
            or context.get("business_context_domain")
        )
        context["business_context_enabled"] = business_context_enabled

        columns = ", ".join(c["name"] for c in context["columns"])
        context["resolved_mappings"] = {}
        context["business_rule"] = None
        context["guardrails"] = None
        context["semantic_context"] = None
        selected_domain = context.get("business_context_domain")
        structured_rules_available = False

        if business_context_enabled and selected_domain:
            context["resolved_mappings"] = resolve_columns(
                available_columns=context["columns"],
                domain=selected_domain
            )
            context["business_rule"] = load_business_rule(
                question=user_input,
                domain=selected_domain
            )
            context["guardrails"] = load_guardrails(domain=selected_domain)
            structured_rules_available = bool(context["resolved_mappings"] or context["business_rule"] or context["guardrails"])
            context["semantic_context"] = build_semantic_context(
                domain=selected_domain,
                question=user_input,
                columns=columns,
                top_k=1
            )
        context["structured_rules_available"] = structured_rules_available
        self.active_file["semantic_context"] = context["semantic_context"]
        self.active_file["semantic_context_hash"] = self._hash_text(context["semantic_context"] or "")

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
        logger.info("BUSINESS_CONTEXT_ENABLED=%s", business_context_enabled)
        logger.info("BUSINESS_CONTEXT_DOMAIN=%s", selected_domain)
        logger.info("STRUCTURED_RULES_AVAILABLE=%s", structured_rules_available)
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
        except Exception as exc:
            logger.exception("PySpark generation failed | phase=PYSPARK_GENERATION")
            return self._build_error_response(
                user_input=user_input,
                context_debug=context_debug,
                analysis_note=analysis_note,
                phase="PYSPARK_GENERATION",
                exc=exc,
            )

        # --------------------------------------------------
        # Step 2 — Execute with up to MAX_RETRIES correction attempts
        # --------------------------------------------------
        last_error = None
        last_code = pyspark_code

        attempts_made = 0

        for attempt in range(1, MAX_RETRIES + 1):
            attempts_made = attempt
            logger.info("Execution attempt %d / %d", attempt, MAX_RETRIES)

            execution = self.executor.execute_query(context, last_code)

            if execution["status"] == "SUCCESS":
                result = execution["result"]

                # Run both LLM insights and explanation in parallel to minimize latency
                from concurrent.futures import ThreadPoolExecutor
                with ThreadPoolExecutor(max_workers=2) as tpool:
                    future_summary = tpool.submit(self.summarizer.summarize, user_input, result, "query")
                    future_explain = tpool.submit(self.summarizer.explain, user_input, result)

                    summary = None
                    text_results = None
                    insights_error = None
                    explanation_error = None

                    try:
                        summary = future_summary.result()
                    except Exception as exc:
                        logger.exception("Reasoning insight generation failed")
                        insights_error = self._format_exception_message(exc)

                    try:
                        text_results = future_explain.result()
                    except Exception as exc:
                        logger.exception("Result explanation generation failed")
                        explanation_error = self._format_exception_message(exc)

                response = {
                    "user_input": user_input,
                    "pyspark": last_code,
                    "results": result,
                    "insights": summary,
                    "text_results": text_results,
                    "analysis_note": analysis_note,
                    "attempts": attempt,
                    "attempts": attempts_made,
                    "context_debug": context_debug,
                }
                if insights_error:
                    response["insights_error"] = insights_error
                if explanation_error:
                    response["text_results_error"] = explanation_error
                if insights_error or explanation_error:
                    response["warning"] = "Result generation completed, but one or more LLM explanation steps failed."
                return response

            # FAILED — capture error and try to correct
            last_error = execution.get("error", "Unknown execution error")

            logger.warning(
                "Execution attempt %d failed: %s", attempt, last_error
            )

            if not self._is_retriable_execution_error(last_error):
                logger.error(
                    "Encountered non-retriable execution error on attempt %d: %s",
                    attempt,
                    last_error
                )
                break

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
            "reason": last_error,
            "query": last_code,
            "pyspark": last_code,
            "results": None,
            "insights": None,
            "text_results": None,
            "analysis_note": analysis_note,
            "attempts": MAX_RETRIES,
            "attempts": attempts_made,
            "context_debug": context_debug,
        }

    def _format_exception_message(self, exc: Exception) -> str:
        if isinstance(exc, LLMInvocationError):
            return f"{exc.error_type}: {str(exc)}"
        return f"{type(exc).__name__}: {str(exc)}"

    def _build_error_response(
        self,
        *,
        user_input: str,
        context_debug: dict[str, Any],
        analysis_note: str | None,
        phase: str,
        exc: Exception,
    ) -> dict[str, Any]:
        error_message = self._format_exception_message(exc)
        return {
            "user_input": user_input,
            "error": error_message,
            "reason": error_message,
            "error_type": getattr(exc, "error_type", type(exc).__name__),
            "phase": phase,
            "query": None,
            "pyspark": None,
            "results": None,
            "insights": None,
            "text_results": None,
            "analysis_note": analysis_note,
            "attempts": 0,
            "context_debug": context_debug,
        }

    def _complete_ingestion(
        self,
        ingestion: dict,
        file_id: str,
        file_path: str,
        file_format: str,
        context: str = None,
        domain: str = None
    ):
        if ingestion["status"] != "SUCCESS":
            raise RuntimeError("Ingestion failed")

        schema = ingestion["schema"]
        profiling = ingestion["profiling"]

        if context:
            schema["semantic_context"] = context
        semantic_context = None
        semantic_context_hash = None
        selected_domain = domain or self._resolve_domain_from_context(context)
        if context:
            logger.info("Wiki root configured for targeted retrieval: %s", context)
            schema["semantic_context_path"] = context
        if selected_domain:
            schema["business_context_domain"] = selected_domain

        uploader = MetadataUploader()
        uploader.upload(schema, file_id)

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

        self.active_file = {
            "file_id": file_id,
            "file_path": file_path,
            "format": file_format,
            "columns": schema["columns"],
            "semantic_context": semantic_context,
            "semantic_context_path": context,
            "semantic_context_hash": semantic_context_hash,
            "business_context_domain": selected_domain
        }

        return profiling

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
            "business_context_enabled": context.get("business_context_enabled", False),
            "business_context_domain": context.get("business_context_domain"),
            "structured_rules_available": context.get("structured_rules_available", False),
            "resolved_mappings": context.get("resolved_mappings") or {},
            "business_rule": context.get("business_rule"),
        }

    def _hash_text(self, text: str) -> str:
        return hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]

    def _resolve_domain_from_context(self, context: str | None) -> str | None:
        if not context:
            return None

        normalized_context = context.replace("\\", "/").rstrip("/")
        for domain, root in settings.DOMAIN_WIKI_ROOTS.items():
            if normalized_context == root.replace("\\", "/").rstrip("/"):
                return domain
        return None

    def _is_retriable_execution_error(self, error_message: str | None) -> bool:
        if not error_message:
            return True

        normalized_error = error_message.lower()
        non_retriable_markers = [
            "modulenotfounderror",
            "no module named",
            "databricks job submission failed",
            "databricks job failed:",
            "cluster_id is not configured",
            "job script paths are not configured",
            "semantic context must be a databricks volume path",
            "no logs returned from databricks",
        ]

        return not any(marker in normalized_error for marker in non_retriable_markers)
