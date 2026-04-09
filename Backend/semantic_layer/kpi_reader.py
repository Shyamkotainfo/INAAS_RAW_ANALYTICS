# Backend/semantic_layer/kpi_reader.py
# Layer 3 — serves pre-aggregated KPIs from gold/semantic tables.
# Answers business metric questions using SQL, not PySpark, at query time.
#
# TODO (Phase C): Implement gold table SQL queries.
# TODO (Phase C): Implement KPI catalog loading.
# TODO (Phase C): Implement natural-language answer formatter.

from typing import Optional
from semantic_layer.kpi_schema import KPIResult, KPICatalog


class KPIReader:
    """
    Reads pre-aggregated KPI results from the semantic / gold layer.

    The semantic layer holds (built once by semantic_build_job.py):
      - Headcount by designation, gender, department, location
      - Average pay by role, gender, period
      - Hiring trend by month / quarter
      - Diversity ratios by department
      - Attrition rates by cohort

    At query time, this module runs a lightweight SQL query against
    the gold table — NOT a full PySpark job on raw data.
    """

    def __init__(self):
        # TODO (Phase C): Initialize SQL/Delta connection to gold tables
        self.catalog: Optional[KPICatalog] = None

    def load_catalog(self) -> KPICatalog:
        """
        Load the registry of all available KPIs.
        Used by the classifier to identify KPI-class questions.
        """
        # TODO (Phase C): Read KPI catalog from config or gold table registry
        raise NotImplementedError("KPIReader.load_catalog() — Phase C implementation pending")

    def get(self, kpi_name: str, filters: dict = None) -> Optional[KPIResult]:
        """
        Retrieve a pre-aggregated KPI result.

        Args:
            kpi_name: KPI identifier, e.g. "avg_pay_by_designation"
            filters:  Optional dict of filter conditions, e.g. {"gender": "Female"}

        Returns:
            KPIResult with the queried data, or None if KPI not found.
        """
        # TODO (Phase C): Execute SQL against the correct gold table
        raise NotImplementedError("KPIReader.get() — Phase C implementation pending")

    def answer(self, question: str, kpi_name: str, filters: dict = None) -> str:
        """
        Answer a KPI-class question from the semantic layer.

        Examples:
            - "What is avg pay for Sr. DevOps Engineers?"
            - "How has headcount grown YoY?"
            - "What is the gender ratio in tech roles?"

        Args:
            question: Original NL question (for LLM formatting)
            kpi_name: Resolved KPI name from classifier
            filters:  Optional dimension filters

        Returns:
            A plain-text answer string.
        """
        # TODO (Phase C): Retrieve KPI result + LLM-format the answer
        raise NotImplementedError("KPIReader.answer() — Phase C implementation pending")
