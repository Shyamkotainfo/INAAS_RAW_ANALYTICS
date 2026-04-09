"""
Interactive NLP console for already-ingested datasets.

Usage:
  python Backend/nlp_console.py --dataset-id cli_a1b2c3
  python Backend/nlp_console.py --dataset-id cli_a1b2c3 --question "What is avg pay by designation?"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Optional

from logger.logger import get_logger

logger = get_logger(__name__)


def _normalize_dataset_id(dataset_id: str) -> str:
    """
    Accepts values like:
      - cli_abef6d
      - cli_abef6d.json
      - schema/cli_abef6d.json
    Returns:
      - cli_abef6d
    """
    cleaned = dataset_id.strip()
    cleaned = cleaned.split("/")[-1]  # drop any prefix path
    if cleaned.lower().endswith(".json"):
        cleaned = cleaned[:-5]
    return cleaned


def _infer_format(file_path: str) -> str:
    ext = Path(file_path).suffix.lower().lstrip(".")
    return ext or "csv"


def _resolve_file_context(
    dataset_id: str,
    file_path: Optional[str],
    file_format: Optional[str],
) -> tuple[str, str]:
    """
    Resolve file_path and file_format.
    Priority: CLI args > metadata from S3.
    """
    # Backward/forward compatible import:
    # some branches expose `load_metadata_from_s3`, others `_load_metadata_from_s3`.
    from core import query_orchestrator as qo

    loader = getattr(qo, "load_metadata_from_s3", None) or getattr(
        qo, "_load_metadata_from_s3", None
    )
    if not loader:
        raise ImportError(
            "Could not find metadata loader in core.query_orchestrator "
            "(expected load_metadata_from_s3 or _load_metadata_from_s3)."
        )

    metadata = loader(dataset_id)
    if not metadata:
        raise FileNotFoundError(
            f"No metadata found in S3 for dataset_id='{dataset_id}'. "
            "Run upload/start-profiling once first."
        )

    meta_path = (
        metadata.get("data_location", {}).get("path")
        or metadata.get("file_path")
        or ""
    )
    meta_format = metadata.get("format") or ""

    resolved_path = file_path or meta_path
    if not resolved_path:
        raise ValueError(
            "Could not infer file_path from metadata. Pass --file-path explicitly."
        )

    resolved_format = file_format or meta_format or _infer_format(resolved_path)
    return resolved_path, resolved_format.lower()


def _print_response(response: dict) -> None:
    route = response.get("route", "UNKNOWN")
    fmt = response.get("format_bucket", "structured")
    print(f"\n[route={route}] [format={fmt}]")

    if response.get("irrelevant"):
        print(response.get("message", "Question is not related to the dataset."))
        return

    if response.get("answer"):
        print(response["answer"])
        return

    if response.get("error"):
        print("Query failed after retries:")
        print(response["error"])
        return

    print(json.dumps(response, indent=2))


def _run_interactive(orchestrator, dataset_id: str) -> None:
    print("\nNLP console ready. Type your question, or 'exit' to quit.")
    while True:
        question = input("\n[NLP] > ").strip()
        if not question:
            continue
        if question.lower() in {"exit", "quit", "q"}:
            print("Bye.")
            break

        try:
            response = orchestrator.run(question, dataset_id)
            _print_response(response)
        except Exception as exc:
            print(f"Query error: {exc}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Ask NLP questions on an already-ingested dataset (no upload step)."
    )
    parser.add_argument(
        "--dataset-id",
        required=True,
        help="Existing dataset id whose metadata/profile already exists in S3.",
    )
    parser.add_argument(
        "--file-path",
        default=None,
        help="Optional override for dataset file path. If omitted, inferred from metadata.",
    )
    parser.add_argument(
        "--file-format",
        default=None,
        help="Optional override for file format (csv/parquet/json).",
    )
    parser.add_argument(
        "--question",
        default=None,
        help="Optional one-shot question. If omitted, starts interactive mode.",
    )
    args = parser.parse_args()
    dataset_id = _normalize_dataset_id(args.dataset_id)

    # Import lazily so --help can work even if env vars are missing.
    from core.query_orchestrator import QueryOrchestrator

    file_path, file_format = _resolve_file_context(
        dataset_id=dataset_id,
        file_path=args.file_path,
        file_format=args.file_format,
    )

    logger.info(
        "Loading context from S3 | dataset_id=%s | file_path=%s | file_format=%s",
        dataset_id,
        file_path,
        file_format,
    )

    orchestrator = QueryOrchestrator()
    orchestrator.load_from_s3(dataset_id, file_path, file_format)

    if args.question:
        response = orchestrator.run(args.question, dataset_id)
        _print_response(response)
        return

    _run_interactive(orchestrator, dataset_id)


if __name__ == "__main__":
    main()
