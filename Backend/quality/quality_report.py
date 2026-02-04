# Backend/quality/quality_report.py

from typing import List, Dict, Any


def empty_report():
    return {
        "status": "PASS",
        "dataset": {},
        "columns": [],
        "issues": []
    }
