# Backend/profile_store/profile_reader.py
# Layer 2 — reads pre-computed column-level statistics.
# Answers distribution/quality questions without PySpark at query time.
#
# TODO (Phase B): Implement S3-backed profile store.
# TODO (Phase B): Implement natural-language answer formatter.

from typing import Optional
from profile_store.profile_schema import DatasetProfile


class ProfileReader:
    """
    Reads pre-computed data profile statistics for a dataset.

    The profile store holds (pre-computed by a one-time Databricks profiling job):
      - Null counts and null percentages per column
      - Distinct value counts per column
      - Min, max, mean, median, stddev for numeric columns
      - Top-N most frequent values for categorical columns
      - Total row and column counts

    PySpark is NOT invoked at query time — stats are pre-computed
    by the profiling_job.py Databricks script during dataset registration.
    """

    def __init__(self):
        # TODO (Phase B): Initialize S3/DynamoDB client for profile retrieval
        pass

    def get(self, dataset_id: str) -> Optional[DatasetProfile]:
        """
        Retrieve stored profile stats for a dataset.

        Args:
            dataset_id: Unique identifier for the dataset.

        Returns:
            DatasetProfile object, or None if not found.
        """
        # TODO (Phase B): Load from S3 path: profile/{dataset_id}/profile.json
        raise NotImplementedError("ProfileReader.get() — Phase B implementation pending")

    def answer(self, dataset_id: str, question: str) -> str:
        """
        Answer a profile-class question from pre-computed stats.

        Examples:
            - "How many nulls are in the DOJ column?"
            - "What is the pay range?"
            - "What are the top 5 designations by headcount?"
            - "Is EmpCode truly unique?"
            - "What is the average age?"

        Args:
            dataset_id: Unique identifier for the dataset.
            question:   The user's natural language question.

        Returns:
            A plain-text answer string.
        """
        # TODO (Phase B): Retrieve profile + LLM-format the answer
        raise NotImplementedError("ProfileReader.answer() — Phase B implementation pending")
