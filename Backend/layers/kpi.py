# Backend/layers/kpi.py

from logger.logger import get_logger
from layers.common import UNSTRUCTURED

logger = get_logger(__name__)

def handle_kpi(question: str, dataset_id: str, format_bucket: str, adhoc_handler_func) -> dict:
    """Answer KPI/business-metric questions from pre-aggregated gold tables."""
    if format_bucket == UNSTRUCTURED:
        return {
            "route": "KPI",
            "format_bucket": format_bucket,
            "answer": (
                "KPI metrics are not available for unstructured documents. "
                "Try a structured CSV or Parquet file."
            ),
            "pyspark": None, "results": None,
        }

    # Phase C TODO: load from semantic_layer.kpi_reader
    logger.info("[L3] KPI gold tables not yet built — falling back to ADHOC PySpark")
    result = adhoc_handler_func(question, dataset_id, format_bucket)
    if result:
        result["route"] = "KPI→ADHOC"
    return result
