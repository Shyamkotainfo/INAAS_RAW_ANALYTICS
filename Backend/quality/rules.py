# Backend/quality/rules.py

INVALID_TOKENS = {"NA", "N/A", "NULL", "", " "}

NULL_THRESHOLD_WARN = 0.20   # 20%
NULL_THRESHOLD_FAIL = 0.50   # 50%

DUPLICATE_THRESHOLD = 0      # strict for raw data
