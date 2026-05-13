# Backend/query_generation/pyspark_generator.py

import json
import re

from logger.logger import get_logger
from llm.llm_query import LLMQuery, invoke_llm
from prompt.pyspark_code_gen import get_pyspark_prompt
from prompt.pyspark_correction import get_correction_prompt
from prompt.relevance_guard import get_relevance_prompt
from pyspark_utils.code_validator import validate_pyspark_code
from prompt.wiki_retriever import load_domain_context_text, try_load_domain_context_text

logger = get_logger(__name__)


class IrrelevantQueryError(Exception):
    """Raised when the user's question is not related to the loaded dataset."""

    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(f"Irrelevant query: {reason}")


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _parse_simple_yaml(text: str) -> dict[str, dict[str, list[str]]]:
    parsed: dict[str, dict[str, list[str]]] = {}
    current_key: str | None = None
    current_list_name: str | None = None

    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.strip().startswith("#"):
            continue

        if not raw_line.startswith(" ") and raw_line.endswith(":"):
            current_key = raw_line[:-1].strip()
            parsed[current_key] = {}
            current_list_name = None
            continue

        if current_key is None:
            continue

        stripped = raw_line.strip()
        if stripped.endswith(":") and not stripped.startswith("-"):
            current_list_name = stripped[:-1]
            parsed[current_key][current_list_name] = []
            continue

        if stripped.startswith("- ") and current_list_name:
            parsed[current_key][current_list_name].append(stripped[2:].strip())

    return parsed


def _load_mappings_config(domain: str) -> dict[str, dict[str, list[str]]]:
    raw = try_load_domain_context_text(domain, "mappings.yaml")
    if not raw:
        logger.info("No mappings.yaml found for domain=%s; continuing without resolved mappings", domain)
        return {}
    config = _parse_simple_yaml(raw)
    if not isinstance(config, dict):
        raise RuntimeError(f"Invalid mappings.yaml for domain={domain}")
    return config


def _load_rules_config(domain: str) -> dict[str, dict]:
    raw = try_load_domain_context_text(domain, "rules.json")
    if not raw:
        logger.info("No rules.json found for domain=%s; continuing without business rules", domain)
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Invalid rules.json for domain={domain}")
    return parsed


def load_guardrails(domain: str = "hr") -> str:
    return try_load_domain_context_text(domain, "guardrails.md") or ""


def resolve_columns(available_columns: list[dict], domain: str = "hr") -> dict[str, str]:
    mappings_config = _load_mappings_config(domain)
    actual_columns = [column["name"] for column in available_columns if column.get("name")]
    normalized_lookup = {_normalize_name(column): column for column in actual_columns}

    resolved: dict[str, str] = {}
    for logical_name, config in mappings_config.items():
        for candidate in config.get("preferred", []):
            matched = normalized_lookup.get(_normalize_name(candidate))
            if matched:
                resolved[logical_name] = matched
                break

    logger.info("RESOLVED_MAPPINGS=%s", json.dumps(resolved, sort_keys=True))
    return resolved


def load_business_rule(question: str, domain: str = "hr") -> dict | None:
    lowered_question = (question or "").lower()
    rules = _load_rules_config(domain)

    best_rule_name = None
    best_score = 0

    for rule_name, rule_config in rules.items():
        score = 0
        for keyword in rule_config.get("keywords", []):
            if keyword.lower() in lowered_question:
                score += max(len(keyword.split()), 1)

        if score > best_score:
            best_score = score
            best_rule_name = rule_name

    if not best_rule_name:
        logger.info("SELECTED_BUSINESS_RULE=None")
        return None

    selected_rule = dict(rules[best_rule_name])
    selected_rule["name"] = best_rule_name
    logger.info("SELECTED_BUSINESS_RULE=%s", json.dumps(selected_rule, sort_keys=True))
    return selected_rule


def _build_prompt_log_summary(
    prompt: str,
    resolved_mappings: dict[str, str],
    business_rule: dict | None,
    preview_chars: int = 400,
) -> str:
    compact_prompt = " ".join(prompt.split())
    preview = compact_prompt[:preview_chars]
    if len(compact_prompt) > preview_chars:
        preview += "..."

    return json.dumps({
        "prompt_chars": len(prompt),
        "rule_name": (business_rule or {}).get("name"),
        "resolved_mappings": resolved_mappings,
        "preview": preview,
    }, sort_keys=True)


class PySparkCodeGenerator:
    def __init__(self):
        self.llm = LLMQuery()

    def generate(self, question: str, context: dict) -> str:
        special_case_code = self._maybe_generate_special_case(question, context)
        if special_case_code:
            logger.info("Using deterministic special-case PySpark for question=%s", question)
            return special_case_code

        columns = self._require_columns(context)
        col_count = len(context["columns"])
        resolved_mappings = context.get("resolved_mappings") or {}
        business_rule = context.get("business_rule")
        guardrails = context.get("guardrails")
        semantic_context = context.get("semantic_context")
        business_context_enabled = context.get("business_context_enabled", False)

        logger.info("QUESTION=%s", question)
        logger.info("AVAILABLE_COLUMNS=%s", columns)
        # Hard stop only when business context is enabled and structured rules exist
        # for the selected domain but none matched the question.
        if business_context_enabled and context.get("structured_rules_available") and not business_rule:
            logger.warning("No business rule found - returning CANNOT_COMPUTE")
            return """
final_df = df.limit(1).select(
    F.lit("CANNOT_COMPUTE").alias("status"),
    F.lit("No business rule defined for this metric").alias("reason")
)
"""

        missing_fields = self._get_missing_required_fields(business_rule, resolved_mappings)
        if missing_fields:
            safe_rule = dict(business_rule)
            safe_rule["missing_required_fields"] = missing_fields
            logger.warning(
                "Business rule missing required fields | rule=%s | missing=%s",
                safe_rule.get("name"),
                missing_fields
            )
            return self._build_cannot_compute_code(
                rule_name=safe_rule.get("name", "unknown_rule"),
                missing_fields=missing_fields
            )

        prompt = get_pyspark_prompt(
            columns=columns,
            question=question,
            resolved_mappings=resolved_mappings,
            business_rule=business_rule,
            guardrails=guardrails,
            semantic_context=semantic_context
        )
        max_tokens = self._get_max_tokens(col_count)

        logger.info(
            "FINAL_PROMPT_SUMMARY=%s",
            _build_prompt_log_summary(prompt, resolved_mappings, business_rule)
        )
        logger.info("Sending PySpark generation prompt to LLM (tokens=%d)", max_tokens)

        try:
            raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        except Exception:
            logger.exception("PySpark LLM generation failed before sanitization")
            raise
        logger.info("Received PySpark code from LLM")
        logger.info("Raw generated PySpark | chars=%d | raw=%s", len(raw), raw)

        code = self._sanitize(raw)
        try:
            self._validate(code, context)
        except RuntimeError as exc:
            logger.warning("Initial PySpark generation failed validation: %s", str(exc))
            code = self._repair_initial_generation(
                question=question,
                columns=columns,
                failing_code=code or raw,
                error_message=str(exc),
                resolved_mappings=resolved_mappings,
                business_rule=business_rule,
                guardrails=guardrails,
                semantic_context=semantic_context,
                max_tokens=max_tokens
            )

        return code

    def correct(self, question: str, context: dict, failing_code: str, error_message: str) -> str:
        special_case_code = self._maybe_generate_special_case(question, context)
        if special_case_code:
            logger.info("Using deterministic special-case PySpark during correction for question=%s", question)
            return special_case_code

        columns = self._require_columns(context)
        col_count = len(context["columns"])

        prompt = get_correction_prompt(
            columns=columns,
            question=question,
            failing_code=failing_code,
            error_message=error_message,
            resolved_mappings=context.get("resolved_mappings") or {},
            business_rule=context.get("business_rule"),
            guardrails=context.get("guardrails"),
            semantic_context=context.get("semantic_context")
        )
        max_tokens = self._get_max_tokens(col_count)

        logger.info(
            "RETRY_PROMPT_SUMMARY=%s",
            _build_prompt_log_summary(
                prompt,
                context.get("resolved_mappings") or {},
                context.get("business_rule")
            )
        )
        logger.info("Sending PySpark correction prompt to LLM (attempt)")
        try:
            raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        except Exception:
            logger.exception("PySpark correction LLM generation failed before sanitization")
            raise
        logger.info("Received corrected PySpark code from LLM")
        logger.info("Raw corrected PySpark | chars=%d | raw=%s", len(raw), raw)

        code = self._sanitize(raw)
        self._validate(code, context)
        return code

    def _check_relevance(self, question: str, columns: str) -> bool:
        prompt = get_relevance_prompt(columns=columns, question=question)

        try:
            response = invoke_llm(
                prompt=prompt,
                temperature=0.0,
                max_tokens=150
            )
            logger.info("Relevance guard response: %s", response)

            text = response.strip()
            if text.startswith("```"):
                lines = text.splitlines()
                text = "\n".join(
                    line for line in lines
                    if not line.strip().startswith("```")
                )

            parsed = json.loads(text)
            return bool(parsed.get("relevant", True))
        except Exception as exc:
            logger.warning("Relevance guard failed to parse response, failing open: %s", str(exc))
            return True

    def _repair_initial_generation(
        self,
        question: str,
        columns: str,
        failing_code: str,
        error_message: str,
        resolved_mappings: dict[str, str],
        business_rule: dict | None,
        guardrails: str | None,
        semantic_context: str | None,
        max_tokens: int,
    ) -> str:
        prompt = get_correction_prompt(
            columns=columns,
            question=question,
            failing_code=failing_code,
            error_message=error_message,
            resolved_mappings=resolved_mappings,
            business_rule=business_rule,
            guardrails=guardrails,
            semantic_context=semantic_context
        )

        logger.info(
            "RETRY_PROMPT_SUMMARY=%s",
            _build_prompt_log_summary(prompt, resolved_mappings, business_rule)
        )
        logger.info("Repairing initial PySpark generation after validation failure")
        try:
            raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        except Exception:
            logger.exception("PySpark repair LLM generation failed before sanitization")
            raise
        logger.info("Raw repaired PySpark | chars=%d | raw=%s", len(raw), raw)

        repaired_code = self._sanitize(raw)
        self._validate(repaired_code)
        return repaired_code

    def _require_columns(self, context: dict) -> str:
        if "columns" not in context or not context["columns"]:
            raise RuntimeError("No columns available in query context")
        return ", ".join(column["name"] for column in context["columns"])

    def _get_max_tokens(self, col_count: int = 0) -> int:
        return min(1200 + (max(0, col_count - 10) // 10 * 100), 4096)

    def _maybe_generate_special_case(self, question: str, context: dict) -> str | None:
        columns = context.get("columns") or []
        actual_columns = [column["name"] for column in columns if column.get("name")]
        normalized_lookup = {_normalize_name(column): column for column in actual_columns}
        lowered_question = (question or "").lower()
        business_rule_name = ((context.get("business_rule") or {}).get("name") or "").strip().lower()

        asks_about_inventory_shortage = (
            business_rule_name == "inventory_shortage"
            or any(
                term in lowered_question
                for term in [
                    "below reorder point",
                    "below safety stock",
                    "reorder point",
                    "safety stock",
                    "low inventory",
                    "inventory shortage",
                ]
            )
        )
        if asks_about_inventory_shortage:
            sku_column = normalized_lookup.get("skuid") or normalized_lookup.get("sku")
            month_column = normalized_lookup.get("month")
            inventory_column = normalized_lookup.get("onhandinventoryunits")
            safety_stock_column = normalized_lookup.get("safetystockunits")
            reorder_point_column = normalized_lookup.get("reorderpointunits")

            if all([
                sku_column,
                month_column,
                inventory_column,
                safety_stock_column,
                reorder_point_column,
            ]):
                return f"""
# 1. columns used: {sku_column}, {month_column}, {inventory_column}, {safety_stock_column}, {reorder_point_column}
# 2. cleaning needed: convert inventory and threshold fields to doubles and normalize month text to a date
# 3. transformation strategy: compare on-hand inventory to safety stock and reorder point, then return only flagged sku-month rows
inventory_shortage_df = (
    df.select(
        F.col("{sku_column}").alias("sku"),
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.regexp_replace(
            F.trim(F.col("{inventory_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("on_hand_inventory_units"),
        F.regexp_replace(
            F.trim(F.col("{safety_stock_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("safety_stock_units"),
        F.regexp_replace(
            F.trim(F.col("{reorder_point_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("reorder_point_units")
    )
    .where(F.col("sku").isNotNull() & (F.trim(F.col("sku")) != ""))
)

flagged_df = (
    inventory_shortage_df
    .withColumn(
        "below_safety_stock_flag",
        F.col("on_hand_inventory_units") < F.col("safety_stock_units")
    )
    .withColumn(
        "below_reorder_point_flag",
        F.col("on_hand_inventory_units") < F.col("reorder_point_units")
    )
    .withColumn(
        "shortage_severity",
        F.when(F.col("on_hand_inventory_units") <= 0, F.lit("critical"))
        .when(
            F.col("below_safety_stock_flag") & F.col("below_reorder_point_flag"),
            F.lit("high")
        )
        .when(
            F.col("below_safety_stock_flag") | F.col("below_reorder_point_flag"),
            F.lit("medium")
        )
        .otherwise(F.lit("normal"))
    )
)

final_df = (
    flagged_df
    .where(F.col("below_safety_stock_flag") | F.col("below_reorder_point_flag"))
    .orderBy(
        F.when(F.col("shortage_severity") == "critical", F.lit(0))
        .when(F.col("shortage_severity") == "high", F.lit(1))
        .otherwise(F.lit(2)),
        F.col("month").asc_nulls_last(),
        F.col("sku").asc()
    )
)
"""

        asks_about_stockout_risk = (
            business_rule_name == "stockout_risk"
            or any(
                term in lowered_question
                for term in [
                    "stockout risk",
                    "stock out risk",
                    "risk of stockout",
                    "run out of stock",
                    "inventory risk",
                    "shortage risk",
                ]
            )
        )
        if asks_about_stockout_risk:
            sku_column = normalized_lookup.get("skuid") or normalized_lookup.get("sku")
            month_column = normalized_lookup.get("month")
            inventory_column = normalized_lookup.get("onhandinventoryunits")
            forecast_column = normalized_lookup.get("forecastunits")
            planned_receipts_column = normalized_lookup.get("plannedreceiptsunits")
            safety_stock_column = normalized_lookup.get("safetystockunits")
            reorder_point_column = normalized_lookup.get("reorderpointunits")
            days_inventory_column = normalized_lookup.get("daysofinventory")
            total_lead_time_column = normalized_lookup.get("totalleadtimedays")
            stockout_flag_column = normalized_lookup.get("stockoutflag")

            if all([
                sku_column,
                month_column,
                inventory_column,
                forecast_column,
            ]):
                planned_receipts_expr = (
                    f"""F.coalesce(
            F.regexp_replace(
                F.trim(F.col("{planned_receipts_column}").cast("string")),
                ",",
                "."
            ).cast("double"),
            F.lit(0.0)
        )"""
                    if planned_receipts_column
                    else "F.lit(0.0)"
                )

                safety_stock_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{safety_stock_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if safety_stock_column
                    else "F.lit(None).cast(\"double\")"
                )

                reorder_point_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{reorder_point_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if reorder_point_column
                    else "F.lit(None).cast(\"double\")"
                )

                days_inventory_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{days_inventory_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if days_inventory_column
                    else "F.lit(None).cast(\"double\")"
                )

                total_lead_time_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{total_lead_time_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if total_lead_time_column
                    else "F.lit(None).cast(\"double\")"
                )

                stockout_flag_expr = (
                    f"""F.when(
            F.lower(F.trim(F.col("{stockout_flag_column}").cast("string"))).isin("true", "1", "yes", "y"),
            F.lit(True)
        ).when(
            F.lower(F.trim(F.col("{stockout_flag_column}").cast("string"))).isin("false", "0", "no", "n"),
            F.lit(False)
        ).otherwise(F.lit(None).cast("boolean"))"""
                    if stockout_flag_column
                    else "F.lit(None).cast(\"boolean\")"
                )

                return f"""
# 1. columns used: {sku_column}, {month_column}, {inventory_column}, {forecast_column}
# 2. cleaning needed: normalize numeric text fields and stockout flags without helper functions
# 3. transformation strategy: keep latest sku snapshot, compute net availability and coverage, then classify high vs medium stockout risk
stockout_risk_df = (
    df.select(
        F.col("{sku_column}").alias("sku"),
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.regexp_replace(
            F.trim(F.col("{inventory_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("on_hand_inventory_units"),
        F.regexp_replace(
            F.trim(F.col("{forecast_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("forecast_units"),
        {planned_receipts_expr}.alias("planned_receipts_units"),
        {safety_stock_expr}.alias("safety_stock_units"),
        {reorder_point_expr}.alias("reorder_point_units"),
        {days_inventory_expr}.alias("days_of_inventory"),
        {total_lead_time_expr}.alias("total_lead_time_days"),
        {stockout_flag_expr}.alias("stockout_flag")
    )
    .where(F.col("sku").isNotNull() & (F.trim(F.col("sku")) != ""))
)

latest_stockout_month_df = (
    stockout_risk_df.groupBy("sku")
    .agg(F.max("month").alias("latest_month"))
)

latest_stockout_df = (
    stockout_risk_df.alias("base")
    .join(
        latest_stockout_month_df.alias("latest"),
        (F.col("base.sku") == F.col("latest.sku"))
        & (F.col("base.month") == F.col("latest.latest_month")),
        "inner"
    )
    .select(
        F.col("base.sku").alias("sku"),
        F.col("base.month").alias("month"),
        F.col("base.on_hand_inventory_units").alias("on_hand_inventory_units"),
        F.col("base.forecast_units").alias("forecast_units"),
        F.col("base.planned_receipts_units").alias("planned_receipts_units"),
        F.col("base.safety_stock_units").alias("safety_stock_units"),
        F.col("base.reorder_point_units").alias("reorder_point_units"),
        F.col("base.days_of_inventory").alias("days_of_inventory"),
        F.col("base.total_lead_time_days").alias("total_lead_time_days"),
        F.col("base.stockout_flag").alias("stockout_flag")
    )
)

evaluated_df = (
    latest_stockout_df
    .withColumn(
        "net_available_units",
        F.coalesce(F.col("on_hand_inventory_units"), F.lit(0.0))
        + F.coalesce(F.col("planned_receipts_units"), F.lit(0.0))
    )
    .withColumn(
        "demand_coverage_ratio",
        F.col("net_available_units") / F.greatest(F.coalesce(F.col("forecast_units"), F.lit(0.0)), F.lit(1.0))
    )
    .withColumn(
        "high_stockout_risk_flag",
        F.coalesce(F.col("stockout_flag"), F.lit(False))
        | (F.col("net_available_units") < F.coalesce(F.col("forecast_units"), F.lit(0.0)))
        | (
            F.col("safety_stock_units").isNotNull()
            & (F.col("on_hand_inventory_units") < F.col("safety_stock_units"))
        )
        | (
            F.col("days_of_inventory").isNotNull()
            & F.col("total_lead_time_days").isNotNull()
            & (F.col("days_of_inventory") < F.col("total_lead_time_days"))
        )
    )
    .withColumn(
        "medium_stockout_risk_flag",
        (F.col("demand_coverage_ratio") >= F.lit(1.0))
        & (F.col("demand_coverage_ratio") <= F.lit(1.2))
        & (
            (
                F.col("safety_stock_units").isNotNull()
                & (F.col("on_hand_inventory_units") <= F.col("safety_stock_units"))
            )
            | (
                F.col("reorder_point_units").isNotNull()
                & (F.col("on_hand_inventory_units") <= F.col("reorder_point_units"))
            )
        )
    )
    .withColumn(
        "stockout_risk_level",
        F.when(F.col("high_stockout_risk_flag"), F.lit("high"))
        .when(F.col("medium_stockout_risk_flag"), F.lit("medium"))
        .otherwise(F.lit("none"))
    )
)

final_df = (
    evaluated_df
    .where(F.col("high_stockout_risk_flag") | F.col("medium_stockout_risk_flag"))
    .orderBy(
        F.when(F.col("stockout_risk_level") == "high", F.lit(0)).otherwise(F.lit(1)),
        F.col("demand_coverage_ratio").asc_nulls_last(),
        F.col("month").asc_nulls_last(),
        F.col("sku").asc()
    )
    .select(
        "sku",
        "month",
        "stockout_risk_level",
        "on_hand_inventory_units",
        "forecast_units",
        "net_available_units",
        "demand_coverage_ratio",
        "stockout_flag"
    )
)
"""

        asks_about_forecast_accuracy = (
            business_rule_name == "forecast_accuracy"
            or any(
                term in lowered_question
                for term in [
                    "forecast demand different from actual demand",
                    "forecast vs actual",
                    "forecast accuracy",
                    "forecast error",
                    "demand variance",
                    "forecast mismatch",
                ]
            )
        )
        if asks_about_forecast_accuracy:
            sku_column = normalized_lookup.get("skuid") or normalized_lookup.get("sku")
            month_column = normalized_lookup.get("month")
            forecast_column = normalized_lookup.get("forecastunits")
            actual_demand_column = normalized_lookup.get("actualdemandunits")

            if all([sku_column, month_column, forecast_column, actual_demand_column]):
                return f"""
# 1. columns used: {sku_column}, {month_column}, {forecast_column}, {actual_demand_column}
# 2. cleaning needed: normalize numeric text fields for forecast and actual demand
# 3. transformation strategy: keep latest sku snapshot, compute forecast variance, then return materially different sku rows
forecast_accuracy_df = (
    df.select(
        F.col("{sku_column}").alias("sku"),
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.regexp_replace(
            F.trim(F.col("{forecast_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("forecast_units"),
        F.regexp_replace(
            F.trim(F.col("{actual_demand_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("actual_demand_units")
    )
    .where(F.col("sku").isNotNull() & (F.trim(F.col("sku")) != ""))
)

latest_forecast_month_df = (
    forecast_accuracy_df.groupBy("sku")
    .agg(F.max("month").alias("latest_month"))
)

latest_forecast_df = (
    forecast_accuracy_df.alias("base")
    .join(
        latest_forecast_month_df.alias("latest"),
        (F.col("base.sku") == F.col("latest.sku"))
        & (F.col("base.month") == F.col("latest.latest_month")),
        "inner"
    )
    .select(
        F.col("base.sku").alias("sku"),
        F.col("base.month").alias("month"),
        F.col("base.forecast_units").alias("forecast_units"),
        F.col("base.actual_demand_units").alias("actual_demand_units")
    )
)

evaluated_df = (
    latest_forecast_df
    .withColumn(
        "forecast_error_units",
        F.col("forecast_units") - F.col("actual_demand_units")
    )
    .withColumn(
        "absolute_error_units",
        F.abs(F.col("forecast_error_units"))
    )
    .withColumn(
        "variance_pct",
        F.col("absolute_error_units") / F.greatest(F.coalesce(F.col("actual_demand_units"), F.lit(0.0)), F.lit(1.0))
    )
    .withColumn(
        "variance_direction",
        F.when(F.col("forecast_units") > F.col("actual_demand_units"), F.lit("overforecast"))
        .otherwise(F.lit("underforecast"))
    )
)

final_df = (
    evaluated_df
    .where(F.col("variance_pct") >= F.lit(0.30))
    .orderBy(
        F.col("variance_pct").desc(),
        F.col("absolute_error_units").desc(),
        F.col("month").asc_nulls_last(),
        F.col("sku").asc()
    )
    .select(
        "sku",
        "month",
        "forecast_units",
        "actual_demand_units",
        "forecast_error_units",
        "absolute_error_units",
        "variance_pct",
        "variance_direction"
    )
)
"""

        asks_about_production_alignment = (
            business_rule_name == "production_alignment"
            or any(
                term in lowered_question
                for term in [
                    "overproduced",
                    "underproduced",
                    "production vs demand",
                    "production alignment",
                    "overproduction",
                    "underproduction",
                ]
            )
        )
        if asks_about_production_alignment:
            sku_column = normalized_lookup.get("skuid") or normalized_lookup.get("sku")
            month_column = normalized_lookup.get("month")
            production_plan_column = normalized_lookup.get("productionplanunits")
            actual_demand_column = normalized_lookup.get("actualdemandunits")
            forecast_column = normalized_lookup.get("forecastunits")

            if all([sku_column, month_column, production_plan_column]) and (actual_demand_column or forecast_column):
                actual_demand_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{actual_demand_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if actual_demand_column
                    else "F.lit(None).cast(\"double\")"
                )
                forecast_expr = (
                    f"""F.regexp_replace(
            F.trim(F.col("{forecast_column}").cast("string")),
            ",",
            "."
        ).cast("double")"""
                    if forecast_column
                    else "F.lit(None).cast(\"double\")"
                )

                return f"""
# 1. columns used: {sku_column}, {month_column}, {production_plan_column}
# 2. cleaning needed: normalize numeric text fields and use actual demand before forecast when available
# 3. transformation strategy: keep latest sku snapshot, compare production plan against demand reference, then classify overproduced vs underproduced
production_alignment_df = (
    df.select(
        F.col("{sku_column}").alias("sku"),
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.regexp_replace(
            F.trim(F.col("{production_plan_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("production_plan_units"),
        {actual_demand_expr}.alias("actual_demand_units"),
        {forecast_expr}.alias("forecast_units")
    )
    .where(F.col("sku").isNotNull() & (F.trim(F.col("sku")) != ""))
)

latest_production_month_df = (
    production_alignment_df.groupBy("sku")
    .agg(F.max("month").alias("latest_month"))
)

latest_production_df = (
    production_alignment_df.alias("base")
    .join(
        latest_production_month_df.alias("latest"),
        (F.col("base.sku") == F.col("latest.sku"))
        & (F.col("base.month") == F.col("latest.latest_month")),
        "inner"
    )
    .select(
        F.col("base.sku").alias("sku"),
        F.col("base.month").alias("month"),
        F.col("base.production_plan_units").alias("production_plan_units"),
        F.col("base.actual_demand_units").alias("actual_demand_units"),
        F.col("base.forecast_units").alias("forecast_units")
    )
)

evaluated_df = (
    latest_production_df
    .withColumn(
        "demand_reference_units",
        F.coalesce(F.col("actual_demand_units"), F.col("forecast_units"))
    )
    .withColumn(
        "demand_reference_type",
        F.when(F.col("actual_demand_units").isNotNull(), F.lit("actual_demand"))
        .otherwise(F.lit("forecast"))
    )
    .withColumn(
        "production_gap_units",
        F.col("production_plan_units") - F.coalesce(F.col("demand_reference_units"), F.lit(0.0))
    )
    .withColumn(
        "production_gap_pct",
        F.col("production_gap_units") / F.greatest(F.coalesce(F.col("demand_reference_units"), F.lit(0.0)), F.lit(1.0))
    )
    .withColumn(
        "production_alignment_status",
        F.when(F.col("production_gap_pct") > F.lit(0.10), F.lit("overproduced"))
        .when(F.col("production_gap_pct") < F.lit(-0.10), F.lit("underproduced"))
        .otherwise(F.lit("balanced"))
    )
)

final_df = (
    evaluated_df
    .where(F.col("production_alignment_status") != "balanced")
    .orderBy(
        F.when(F.col("production_alignment_status") == "underproduced", F.lit(0)).otherwise(F.lit(1)),
        F.abs(F.col("production_gap_pct")).desc(),
        F.col("month").asc_nulls_last(),
        F.col("sku").asc()
    )
    .select(
        "sku",
        "month",
        "production_alignment_status",
        "production_plan_units",
        "demand_reference_units",
        "demand_reference_type",
        "production_gap_units",
        "production_gap_pct"
    )
)
"""

        asks_about_inventory_support = (
            business_rule_name == "inventory_supports_production"
            or any(
                term in lowered_question
                for term in [
                    "inventory support the next production plan",
                    "inventory supports production",
                    "support the production plan",
                    "enough inventory for production",
                    "inventory coverage for production",
                ]
            )
        )
        if asks_about_inventory_support:
            sku_column = normalized_lookup.get("skuid") or normalized_lookup.get("sku")
            month_column = normalized_lookup.get("month")
            inventory_column = normalized_lookup.get("onhandinventoryunits")
            production_plan_column = normalized_lookup.get("productionplanunits")
            planned_receipts_column = normalized_lookup.get("plannedreceiptsunits")
            replenishment_column = normalized_lookup.get("replenishmentorderunits")

            if all([sku_column, month_column, inventory_column, production_plan_column]):
                planned_receipts_expr = (
                    f"""F.coalesce(
            F.regexp_replace(
                F.trim(F.col("{planned_receipts_column}").cast("string")),
                ",",
                "."
            ).cast("double"),
            F.lit(0.0)
        )"""
                    if planned_receipts_column
                    else "F.lit(0.0)"
                )
                replenishment_expr = (
                    f"""F.coalesce(
            F.regexp_replace(
                F.trim(F.col("{replenishment_column}").cast("string")),
                ",",
                "."
            ).cast("double"),
            F.lit(0.0)
        )"""
                    if replenishment_column
                    else "F.lit(0.0)"
                )

                return f"""
# 1. columns used: {sku_column}, {month_column}, {inventory_column}, {production_plan_column}
# 2. cleaning needed: normalize inventory, inbound supply, and production plan numeric text fields
# 3. transformation strategy: keep latest sku snapshot, compute available inventory for plan, then return unsupported sku rows
inventory_support_df = (
    df.select(
        F.col("{sku_column}").alias("sku"),
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.regexp_replace(
            F.trim(F.col("{inventory_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("on_hand_inventory_units"),
        F.regexp_replace(
            F.trim(F.col("{production_plan_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("production_plan_units"),
        {planned_receipts_expr}.alias("planned_receipts_units"),
        {replenishment_expr}.alias("replenishment_order_units")
    )
    .where(F.col("sku").isNotNull() & (F.trim(F.col("sku")) != ""))
)

latest_inventory_support_month_df = (
    inventory_support_df.groupBy("sku")
    .agg(F.max("month").alias("latest_month"))
)

latest_inventory_support_df = (
    inventory_support_df.alias("base")
    .join(
        latest_inventory_support_month_df.alias("latest"),
        (F.col("base.sku") == F.col("latest.sku"))
        & (F.col("base.month") == F.col("latest.latest_month")),
        "inner"
    )
    .select(
        F.col("base.sku").alias("sku"),
        F.col("base.month").alias("month"),
        F.col("base.on_hand_inventory_units").alias("on_hand_inventory_units"),
        F.col("base.production_plan_units").alias("production_plan_units"),
        F.col("base.planned_receipts_units").alias("planned_receipts_units"),
        F.col("base.replenishment_order_units").alias("replenishment_order_units")
    )
)

evaluated_df = (
    latest_inventory_support_df
    .withColumn(
        "available_for_plan_units",
        F.coalesce(F.col("on_hand_inventory_units"), F.lit(0.0))
        + F.coalesce(F.col("planned_receipts_units"), F.lit(0.0))
        + F.coalesce(F.col("replenishment_order_units"), F.lit(0.0))
    )
    .withColumn(
        "inventory_gap_units",
        F.col("available_for_plan_units") - F.coalesce(F.col("production_plan_units"), F.lit(0.0))
    )
    .withColumn(
        "support_status",
        F.when(F.col("inventory_gap_units") >= F.lit(0.0), F.lit("supported"))
        .otherwise(F.lit("unsupported"))
    )
    .withColumn(
        "shortfall_units",
        F.when(F.col("inventory_gap_units") < F.lit(0.0), F.abs(F.col("inventory_gap_units")))
        .otherwise(F.lit(0.0))
    )
)

final_df = (
    evaluated_df
    .where(F.col("support_status") == "unsupported")
    .orderBy(
        F.col("shortfall_units").desc(),
        F.col("month").asc_nulls_last(),
        F.col("sku").asc()
    )
    .select(
        "sku",
        "month",
        "on_hand_inventory_units",
        "planned_receipts_units",
        "replenishment_order_units",
        "available_for_plan_units",
        "production_plan_units",
        "inventory_gap_units",
        "shortfall_units",
        "support_status"
    )
)
"""

        asks_about_capacity_support = (
            business_rule_name == "capacity_supports_demand"
            or any(
                term in lowered_question
                for term in [
                    "production capacity meet forecasted demand",
                    "capacity meets forecast demand",
                    "capacity support demand",
                    "forecasted demand",
                    "capacity feasibility",
                    "enough capacity",
                ]
            )
        )
        if asks_about_capacity_support:
            month_column = normalized_lookup.get("month")
            line_column = normalized_lookup.get("manufacturingline")
            forecast_column = normalized_lookup.get("forecastunits")
            capacity_column = normalized_lookup.get("productioncapacityunits")

            if all([month_column, line_column, forecast_column, capacity_column]):
                return f"""
# 1. columns used: {month_column}, {line_column}, {forecast_column}, {capacity_column}
# 2. cleaning needed: normalize month text to date and numeric text fields for forecast and capacity
# 3. transformation strategy: aggregate forecast by line-month, use stable max capacity by line-month, then return constrained rows
capacity_support_df = (
    df.select(
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.col("{line_column}").alias("manufacturing_line"),
        F.regexp_replace(
            F.trim(F.col("{forecast_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("forecast_units"),
        F.regexp_replace(
            F.trim(F.col("{capacity_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("production_capacity_units")
    )
    .where(
        F.col("manufacturing_line").isNotNull()
        & (F.trim(F.col("manufacturing_line")) != "")
    )
)

aggregated_df = (
    capacity_support_df.groupBy("month", "manufacturing_line")
    .agg(
        F.sum(F.coalesce(F.col("forecast_units"), F.lit(0.0))).alias("aggregated_forecast_units"),
        F.max(F.coalesce(F.col("production_capacity_units"), F.lit(0.0))).alias("aggregated_capacity_units")
    )
)

evaluated_df = (
    aggregated_df
    .withColumn(
        "required_capacity_utilization_pct",
        F.lit(100.0) * F.col("aggregated_forecast_units") / F.greatest(F.col("aggregated_capacity_units"), F.lit(1.0))
    )
    .withColumn(
        "capacity_status",
        F.when(F.col("aggregated_capacity_units") >= F.col("aggregated_forecast_units"), F.lit("feasible"))
        .otherwise(F.lit("constrained"))
    )
    .withColumn(
        "capacity_shortfall_units",
        F.when(
            F.col("aggregated_capacity_units") < F.col("aggregated_forecast_units"),
            F.col("aggregated_forecast_units") - F.col("aggregated_capacity_units")
        ).otherwise(F.lit(0.0))
    )
)

final_df = (
    evaluated_df
    .where(F.col("capacity_status") == "constrained")
    .orderBy(
        F.col("capacity_shortfall_units").desc(),
        F.col("month").asc_nulls_last(),
        F.col("manufacturing_line").asc()
    )
    .select(
        "month",
        "manufacturing_line",
        "aggregated_forecast_units",
        "aggregated_capacity_units",
        "required_capacity_utilization_pct",
        "capacity_shortfall_units",
        "capacity_status"
    )
)
"""

        asks_about_line_load = (
            business_rule_name == "capacity_usage"
            or (
                "line" in lowered_question
                and any(term in lowered_question for term in ["underutilized", "overloaded", "utilization", "capacity"])
            )
        )
        if not asks_about_line_load:
            return None

        line_column = normalized_lookup.get("manufacturingline")
        month_column = normalized_lookup.get("month")
        utilization_column = normalized_lookup.get("capacityutilizationpct")
        if not line_column or not utilization_column:
            return None

        if month_column:
            return f"""
# 1. columns used: {month_column}, {line_column}, {utilization_column}
# 2. cleaning needed: normalize month text to date and utilization text values like "26,205" to doubles
# 3. transformation strategy: aggregate utilization by manufacturing line and month, then classify full capacity vs overloaded vs underutilized
line_utilization_df = (
    df.select(
        F.coalesce(
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd HH:mm:ss"
            ).cast("date"),
            F.to_timestamp(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd'T'HH:mm:ss"
            ).cast("date"),
            F.to_date(
                F.trim(F.col("{month_column}").cast("string")),
                "yyyy-MM-dd"
            )
        ).alias("month"),
        F.col("{line_column}").alias("manufacturing_line"),
        F.regexp_replace(
            F.trim(F.col("{utilization_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("capacity_utilization_pct")
    )
    .where(
        F.col("manufacturing_line").isNotNull()
        & (F.trim(F.col("manufacturing_line")) != "")
        & F.col("capacity_utilization_pct").isNotNull()
    )
)

aggregated_df = (
    line_utilization_df.groupBy("month", "manufacturing_line")
    .agg(
        F.round(F.avg("capacity_utilization_pct"), 2).alias("avg_capacity_utilization_pct"),
        F.round(F.max("capacity_utilization_pct"), 2).alias("peak_capacity_utilization_pct"),
        F.count(F.lit(1)).alias("observations")
    )
)

classified_df = (
    aggregated_df.withColumn(
        "status",
        F.when(F.col("avg_capacity_utilization_pct") >= 95, F.lit("full_capacity"))
        .when(F.col("peak_capacity_utilization_pct") >= 85, F.lit("overloaded"))
        .when(F.col("avg_capacity_utilization_pct") < 50, F.lit("underutilized"))
        .otherwise(F.lit("balanced"))
    )
    .withColumn(
        "classification_basis",
        F.when(
            F.col("status") == "full_capacity",
            F.lit("average utilization >= 95%")
        )
        .when(
            F.col("status") == "overloaded",
            F.lit("peak utilization >= 85%")
        )
        .when(
            F.col("status") == "underutilized",
            F.lit("average utilization < 50%")
        )
        .otherwise(F.lit("utilization within 50% to 95% band"))
    )
)

final_df = (
    classified_df
    .where(F.col("status") != "balanced")
    .orderBy(
        F.col("month").asc_nulls_last(),
        F.when(F.col("status") == "full_capacity", F.lit(0))
        .when(F.col("status") == "overloaded", F.lit(1))
        .otherwise(F.lit(2)),
        F.col("peak_capacity_utilization_pct").desc(),
        F.col("manufacturing_line").asc()
    )
)
"""

        return f"""
# 1. columns used: {line_column}, {utilization_column}
# 2. cleaning needed: normalize utilization text values like "26,205" to doubles and drop nulls
# 3. transformation strategy: aggregate utilization by manufacturing line and classify full capacity vs overloaded vs underutilized
line_utilization_df = (
    df.select(
        F.col("{line_column}").alias("manufacturing_line"),
        F.regexp_replace(
            F.trim(F.col("{utilization_column}").cast("string")),
            ",",
            "."
        ).cast("double").alias("capacity_utilization_pct")
    )
    .where(
        F.col("manufacturing_line").isNotNull()
        & (F.trim(F.col("manufacturing_line")) != "")
        & F.col("capacity_utilization_pct").isNotNull()
    )
)

aggregated_df = (
    line_utilization_df.groupBy("manufacturing_line")
    .agg(
        F.round(F.avg("capacity_utilization_pct"), 2).alias("avg_capacity_utilization_pct"),
        F.round(F.max("capacity_utilization_pct"), 2).alias("peak_capacity_utilization_pct"),
        F.count(F.lit(1)).alias("observations")
    )
)

classified_df = (
    aggregated_df.withColumn(
        "status",
        F.when(F.col("avg_capacity_utilization_pct") >= 95, F.lit("full_capacity"))
        .when(F.col("peak_capacity_utilization_pct") >= 85, F.lit("overloaded"))
        .when(F.col("avg_capacity_utilization_pct") < 50, F.lit("underutilized"))
        .otherwise(F.lit("balanced"))
    )
    .withColumn(
        "classification_basis",
        F.when(
            F.col("status") == "full_capacity",
            F.lit("average utilization >= 95%")
        )
        .when(
            F.col("status") == "overloaded",
            F.lit("peak utilization >= 85%")
        )
        .when(
            F.col("status") == "underutilized",
            F.lit("average utilization < 50%")
        )
        .otherwise(F.lit("utilization within 50% to 95% band"))
    )
)

final_df = (
    classified_df.where(F.col("status") != "balanced")
    .orderBy(
        F.when(F.col("status") == "full_capacity", F.lit(0))
        .when(F.col("status") == "overloaded", F.lit(1))
        .otherwise(F.lit(2)),
        F.col("peak_capacity_utilization_pct").desc(),
        F.col("avg_capacity_utilization_pct").asc(),
        F.col("manufacturing_line").asc()
    )
)
"""
    
    def _sanitize(self, code: str) -> str:
        from pyspark_utils.code_sanitizer import sanitize
        return sanitize(code)

    # --------------------------------------------------
    # PRIVATE: validate generated code
    # --------------------------------------------------
    def _validate(self, code: str, context: dict | None = None) -> None:
        if not code:
            raise RuntimeError("Generated PySpark code is empty after sanitization")
        if "final_df" not in code:
            logger.error("Invalid PySpark code - missing final_df:\n%s", code)
            raise RuntimeError("Generated PySpark code is missing final_df assignment")
        if "crossJoin" in code:
            raise RuntimeError("Generated PySpark code contains forbidden crossJoin")
        if "spark.sql(" in code:
            raise RuntimeError("Generated PySpark code contains forbidden spark.sql()")
        if "df.count()" in code:
            raise RuntimeError("Generated PySpark code uses df.count() which is forbidden")

        available_columns = None
        if context and context.get("columns"):
            available_columns = [c["name"] for c in context.get("columns", [])]

        validate_pyspark_code(code, available_columns=available_columns)
    def _get_missing_required_fields(
        self,
        business_rule: dict | None,
        resolved_mappings: dict[str, str]
    ) -> list[str]:
        if not business_rule:
            return []

        missing = []
        for field in business_rule.get("required_fields", []):
            if field not in resolved_mappings:
                missing.append(field)

        required_any_of = business_rule.get("required_any_of", [])
        if required_any_of and not any(field in resolved_mappings for field in required_any_of):
            missing.append("one_of(" + ", ".join(required_any_of) + ")")

        return missing

    def _build_cannot_compute_code(self, rule_name: str, missing_fields: list[str]) -> str:
        missing_text = ", ".join(missing_fields)
        return "\n".join([
            f"# 1. columns used: none; missing logical fields for {rule_name}",
            "# 2. cleaning needed: none",
            "# 3. transformation strategy: return a one-row CANNOT_COMPUTE result",
            "final_df = df.limit(1).select(",
            '    F.lit("CANNOT_COMPUTE").alias("status"),',
            f'    F.lit("Missing required fields for {rule_name}: {missing_text}").alias("reason")',
            ")"
        ])
