# Backend/metadata_store/metadata_reader.py
# Layer 1 — reads pre-stored dataset metadata.
# Answers schema/structural questions without PySpark at query time.
#
# TODO (Phase A): Implement S3-backed store.
# TODO (Phase A): Implement business glossary lookup.

from typing import Optional
from metadata_store.metadata_schema import DatasetMetadata


class MetadataReader:
    """
    Reads dataset metadata from the metadata store.

    The metadata store holds:
      - Column names, data types, nullable flags
      - Dimension vs measure classification
      - Table relationships (FK→PK in star/snowflake)
      - Business glossary
      - Table grain description

    PySpark is NOT invoked at query time — metadata is pre-stored
    during dataset registration (attach_file flow).
    """

    def __init__(self):
        # TODO (Phase A): Initialize S3/DynamoDB client for metadata retrieval
        pass

    def get(self, dataset_id: str) -> Optional[DatasetMetadata]:
        """
        Retrieve stored metadata for a dataset.

        Args:
            dataset_id: Unique identifier for the dataset.

        Returns:
            DatasetMetadata object, or None if not found.
        """
        # TODO (Phase A): Load from S3 path: schema/{dataset_id}/metadata.json
        raise NotImplementedError("MetadataReader.get() — Phase A implementation pending")

    def answer(self, dataset_id: str, question: str) -> str:
        """
        Answer a metadata-class question directly from stored schema.

        Examples:
            - "What columns does this table have?"
            - "Which columns are dimensions vs measures?"
            - "What is the grain of this dataset?"
            - "What does Dummy Pay mean?"

        Args:
            dataset_id: Unique identifier for the dataset.
            question:   The user's natural language question.

        Returns:
            A plain-text answer string.
        """
        # TODO (Phase A): Retrieve metadata + LLM-format the answer
        raise NotImplementedError("MetadataReader.answer() — Phase A implementation pending")
