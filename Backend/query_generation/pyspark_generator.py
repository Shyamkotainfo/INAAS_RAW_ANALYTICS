# Backend/query_generation/pyspark_generator.py

import json
import re

from logger.logger import get_logger
from llm.llm_query import LLMQuery, invoke_llm
from prompt.pyspark_code_gen import get_pyspark_prompt
from prompt.pyspark_correction import get_correction_prompt
from prompt.relevance_guard import get_relevance_prompt
from pyspark_utils.code_validator import validate_pyspark_code
from prompt.wiki_retriever import load_domain_context_text

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
    raw = load_domain_context_text(domain, "mappings.yaml")
    config = _parse_simple_yaml(raw)
    if not isinstance(config, dict):
        raise RuntimeError(f"Invalid mappings.yaml for domain={domain}")
    return config


def _load_rules_config(domain: str) -> dict[str, dict]:
    raw = load_domain_context_text(domain, "rules.json")
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Invalid rules.json for domain={domain}")
    return parsed


def load_guardrails(domain: str = "hr") -> str:
    return load_domain_context_text(domain, "guardrails.md")


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


class PySparkCodeGenerator:
    def __init__(self):
        self.llm = LLMQuery()

    def generate(self, question: str, context: dict) -> str:
        columns = self._require_columns(context)
        col_count = len(context["columns"])
        resolved_mappings = context.get("resolved_mappings") or {}
        business_rule = context.get("business_rule")
        guardrails = context.get("guardrails")
        semantic_context = context.get("semantic_context")
        resolved_terms = context.get("resolved_terms")

        logger.info("QUESTION=%s", question)
        logger.info("AVAILABLE_COLUMNS=%s", columns)

        prompt = get_pyspark_prompt(columns, question, semantic_context, resolved_terms)
        max_tokens = self._get_max_tokens(question, col_count)
        missing_fields = self._get_missing_required_fields(business_rule, resolved_mappings)
        if business_rule and missing_fields:
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

        logger.info("FINAL_PROMPT=%s", prompt)
        logger.info("Sending PySpark generation prompt to LLM (tokens=%d)", max_tokens)
        raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        logger.info("Received PySpark code from LLM")

        print("\n========== GENERATED PYSPARK ==========\n")
        print(raw)
        print("\n=======================================\n")

        code = self._sanitize(raw)
        self._validate(code, context)
        try:
            self._validate(code)
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
        columns = self._require_columns(context)
        col_count = len(context["columns"])
        semantic_context = context.get("semantic_context")
        resolved_terms = context.get("resolved_terms")

        prompt = get_correction_prompt(
            columns=columns,
            question=question,
            failing_code=failing_code,
            error_message=error_message,
            semantic_context=semantic_context,
            resolved_terms=resolved_terms
        )

        max_tokens = self._get_max_tokens(question, col_count)

        logger.info(
            "LLM correction prompt built | semantic_context_present=%s | context_chars=%d | marker_in_prompt=%s | prompt_chars=%d",
            bool(semantic_context),
            len(semantic_context or ""),
            "CONTEXT_MARKER" in prompt,
            len(prompt)
            resolved_mappings=context.get("resolved_mappings") or {},
            business_rule=context.get("business_rule"),
            guardrails=context.get("guardrails"),
            semantic_context=context.get("semantic_context")
        )
        max_tokens = self._get_max_tokens(col_count)

        logger.info("RETRY_PROMPT=%s", prompt)
        logger.info("Sending PySpark correction prompt to LLM (attempt)")
        raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        logger.info("Received corrected PySpark code from LLM")

        print("\n========== CORRECTED PYSPARK ==========\n")
        print(raw)
        print("\n=======================================\n")

        code = self._sanitize(raw)
        self._validate(code, context)

        self._validate(code)
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

        logger.info("RETRY_PROMPT=%s", prompt)
        logger.info("Repairing initial PySpark generation after validation failure")
        raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()

        print("\n========== REPAIRED INITIAL PYSPARK ==========\n")
        print(raw)
        print("\n==============================================\n")

        repaired_code = self._sanitize(raw)
        self._validate(repaired_code)
        return repaired_code

    def _require_columns(self, context: dict) -> str:
        if "columns" not in context or not context["columns"]:
            raise RuntimeError("No columns available in query context")
        return ", ".join(column["name"] for column in context["columns"])

    def _get_max_tokens(self, col_count: int = 0) -> int:
        return min(1200 + (max(0, col_count - 10) // 10 * 100), 4096)

    def _sanitize(self, code: str) -> str:
        cleaned = []
        for line in code.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("```"):
                continue
            if stripped.startswith(("from ", "import ")):
                continue
            if stripped.lower().startswith(("here", "this code", "the following", "note:")):
                continue
            cleaned.append(stripped)

        return "\n".join(cleaned)

    # --------------------------------------------------
    # PRIVATE: validate generated code
    # --------------------------------------------------
    def _validate(self, code: str, context: dict) -> None:
        """
        Raise RuntimeError if the code is structurally invalid.
        """
    def _validate(self, code: str) -> None:
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

        validate_pyspark_code(
            code,
            available_columns=[c["name"] for c in context.get("columns", [])]
        )
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
