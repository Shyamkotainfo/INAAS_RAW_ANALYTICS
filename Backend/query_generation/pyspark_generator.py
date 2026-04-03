# Backend/query_generation/pyspark_generator.py

import json
from logger.logger import get_logger
from llm.llm_query import LLMQuery, invoke_llm
from prompt.pyspark_code_gen import get_pyspark_prompt
from prompt.pyspark_correction import get_correction_prompt
from prompt.relevance_guard import get_relevance_prompt

logger = get_logger(__name__)


# =====================================================
# CUSTOM EXCEPTIONS
# =====================================================

class IrrelevantQueryError(Exception):
    """Raised when the user's question is not related to the loaded dataset."""
    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(f"Irrelevant query: {reason}")


# =====================================================
# MODE DETECTION KEYWORDS (for token budget heuristic)
# =====================================================

_META_KEYWORDS = {
    "dimension", "dimensions", "measure", "measures", "grain", "what does one row",
    "cardinality", "date column", "time range", "data quality", "nulls", "null",
    "duplicates", "foreign key", "schema", "overview", "profile", "profiling",
    "hierarchy", "hierarchies", "aggregation level", "granularity", "rolled up",
    "gaps in time", "missing days", "missing weeks", "time coverage", "kpi", "kpis",
    "metrics can i derive", "business metrics", "primary business", "what columns",
    "what can i slice", "what can i aggregate", "what can i measure", "data types",
    "identify", "identifiers", "categorical", "continuous", "encoded", "lookup",
    "parent child", "drill down", "is this raw",
}


# =====================================================
# GENERATOR
# =====================================================

class PySparkCodeGenerator:
    def __init__(self):
        self.llm = LLMQuery()

    # --------------------------------------------------
    # PUBLIC: generate  (first-time code generation)
    # --------------------------------------------------
    def generate(self, question: str, context: dict) -> str:
        """
        Generate PySpark code for a question.

        Raises:
            IrrelevantQueryError  — if the question is not related to the dataset.
            RuntimeError          — if valid PySpark code cannot be produced.
        """
        if "columns" not in context or not context["columns"]:
            raise RuntimeError("No columns available in query context")

        columns = ", ".join(c["name"] for c in context["columns"])
        col_count = len(context["columns"])

        # ---- Relevance Guard ----
        if not self._check_relevance(question, columns):
            raise IrrelevantQueryError(
                "The question does not relate to the available dataset columns."
            )

        prompt = get_pyspark_prompt(columns, question)
        max_tokens = self._get_max_tokens(question, col_count)

        logger.info("Sending PySpark generation prompt to LLM (tokens=%d)", max_tokens)
        raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        logger.info("Received PySpark code from LLM")

        print("\n========== GENERATED PYSPARK ==========\n")
        print(raw)
        print("\n=======================================\n")

        code = self._sanitize(raw)
        self._validate(code)

        return code

    # --------------------------------------------------
    # PUBLIC: correct  (LLM-based correction on retry)
    # --------------------------------------------------
    def correct(self, question: str, context: dict, failing_code: str, error_message: str) -> str:
        """
        Ask the LLM to fix failing PySpark code given the error message.

        Returns corrected code string.
        Raises RuntimeError if the corrected code is still invalid.
        """
        columns = ", ".join(c["name"] for c in context["columns"])
        col_count = len(context["columns"])

        prompt = get_correction_prompt(
            columns=columns,
            question=question,
            failing_code=failing_code,
            error_message=error_message
        )

        max_tokens = self._get_max_tokens(question, col_count)

        logger.info("Sending PySpark correction prompt to LLM (attempt)")
        raw = self.llm.generate(prompt, max_tokens=max_tokens).strip()
        logger.info("Received corrected PySpark code from LLM")

        print("\n========== CORRECTED PYSPARK ==========\n")
        print(raw)
        print("\n=======================================\n")

        code = self._sanitize(raw)
        self._validate(code)

        return code

    # --------------------------------------------------
    # PRIVATE: relevance check
    # --------------------------------------------------
    def _check_relevance(self, question: str, columns: str) -> bool:
        """
        Call the LLM relevance guard. Returns True if relevant, False if not.
        Falls back to True on any parsing error (fail open).
        """
        prompt = get_relevance_prompt(columns=columns, question=question)

        try:
            response = invoke_llm(
                prompt=prompt,
                temperature=0.0,
                max_tokens=150
            )

            logger.info("Relevance guard response: %s", response)

            # Extract JSON from the response
            # The LLM should return {"relevant": true/false, "reason": "..."}
            text = response.strip()

            # Strip markdown code fences if present
            if text.startswith("```"):
                lines = text.splitlines()
                text = "\n".join(
                    line for line in lines
                    if not line.strip().startswith("```")
                )

            parsed = json.loads(text)
            relevant = parsed.get("relevant", True)

            if not relevant:
                logger.info(
                    "Relevance guard blocked query. Reason: %s",
                    parsed.get("reason", "unknown")
                )

            return bool(relevant)

        except Exception as e:
            # Fail open — if we can't parse the guard response, allow generation
            logger.warning("Relevance guard failed to parse response, failing open: %s", str(e))
            return True

    # --------------------------------------------------
    # PRIVATE: dynamic token budget
    # --------------------------------------------------
    def _get_max_tokens(self, question: str, col_count: int = 0) -> int:
        lowered = question.lower()
        is_meta = any(kw in lowered for kw in _META_KEYWORDS)

        # Base: 1000 for analytical, 1800 for meta (more complex template code)
        base = 1800 if is_meta else 1000

        # Scale up for wide schemas — every 10 extra columns adds 100 tokens
        extra = max(0, col_count - 10) // 10 * 100

        return min(base + extra, 4096)  # cap at 4096

    # --------------------------------------------------
    # PRIVATE: sanitize — remove LLM chatter but KEEP # comments
    # --------------------------------------------------
    def _sanitize(self, code: str) -> str:
        """
        Remove markdown artifacts and conversational text.
        Preserves # comment lines (chain-of-thought reasoning).
        """
        cleaned = []
        for line in code.splitlines():
            stripped = line.strip()

            if not stripped:
                continue

            # Remove markdown code fences
            if stripped.startswith("```"):
                continue

            # Remove import statements (the environment provides everything)
            if stripped.startswith(("from ", "import ")):
                continue

            # Remove conversational filler lines
            if stripped.lower().startswith(("here", "this code", "the following", "note:")):
                continue

            # KEEP everything else, including # comment lines
            cleaned.append(stripped)

        return "\n".join(cleaned)

    # --------------------------------------------------
    # PRIVATE: validate generated code
    # --------------------------------------------------
    def _validate(self, code: str) -> None:
        """
        Raise RuntimeError if the code is structurally invalid.
        """
        if not code:
            raise RuntimeError("Generated PySpark code is empty after sanitization")

        if "final_df" not in code:
            logger.error("Invalid PySpark code — missing final_df:\n%s", code)
            raise RuntimeError("Generated PySpark code is missing final_df assignment")

        if "crossJoin" in code:
            raise RuntimeError("Generated PySpark code contains forbidden crossJoin")

        if "spark.sql(" in code:
            raise RuntimeError("Generated PySpark code contains forbidden spark.sql()")

        # Ensure df.count() is not used (should be df.select(F.count("*")))
        if "df.count()" in code:
            raise RuntimeError("Generated PySpark code uses df.count() which is forbidden")
