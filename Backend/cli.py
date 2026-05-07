from core.query_orchestrator import QueryOrchestrator
from analytics.dataset_overview_generator import DatasetOverviewGenerator
from config.settings import settings
import json
import shlex
import uuid


def detect_format(path: str):
    path = path.lower()

    if path.endswith(".csv"):
        return "csv"
    if path.endswith(".parquet"):
        return "parquet"
    if path.endswith(".json"):
        return "json"

    raise ValueError("Unsupported format")


def main():
    print("\n" + "=" * 60)
    print("INAAS - RAW Mode (CLI)")
    print("=" * 60)
    print("Commands:")
    print("  upload local <local_file_path>")
    print("  upload url <volume_path>")
    print("  ask natural language question")
    print("  exit")
    print("=" * 60)

    orchestrator = QueryOrchestrator()
    overview_generator = DatasetOverviewGenerator()

    volume_base = settings.databricks_volume_base
    current_dataset_id = None

    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in {"exit", "quit", "q"}:
                print("Goodbye!")
                break

            if user_input.startswith("upload "):
                stripped = user_input.strip()
                if not stripped.lower().startswith("upload "):
                    print("Invalid command.")
                    continue

                remainder = stripped[7:].strip()
                if not remainder:
                    print("Invalid command.")
                    print("Use:")
                    print("  upload local <file_path>")
                    print("  upload url <volume_path>")
                    continue

                mode, separator, path_part = remainder.partition(" ")
                mode = mode.lower()
                path_part = path_part.strip()

                if not separator or not path_part:
                    print("Invalid command.")
                    print("Use:")
                    print("  upload local <file_path>")
                    print("  upload url <volume_path>")
                    continue

                selected_domain = None
                semantic_context = None

                if mode == "local":
                    path = path_part.strip().strip('"')

                    try:
                        with open(path, "rb"):
                            pass
                    except Exception as e:
                        print(f"Failed to read local file: {e}")
                        continue

                    file_format = detect_format(path)

                    print("\nUploading file to Databricks Volume...")
                    volume_path = orchestrator.executor.upload_to_volume(
                        local_path=path,
                        volume_base_path=volume_base
                    )
                    print(f"Uploaded to: {volume_path}")

                    print("\nSelect Business Context:")
                    print("1. HR")
                    print("2. Manufacturing")
                    print("3. None")

                    choice = input("Enter choice: ").strip()
                    domain_map = {
                        "1": "hr",
                        "2": "manufacturing",
                        "3": None,
                    }
                    selected_domain = domain_map.get(choice)

                    if choice not in domain_map:
                        print("Invalid choice. Defaulting to no context.")

                    if selected_domain:
                        semantic_context = settings.DOMAIN_WIKI_ROOTS.get(selected_domain)

                elif mode == "url":
                    volume_path = path_part.strip().strip('"')
                    file_format = detect_format(volume_path)

                else:
                    print("Invalid mode. Use 'local' or 'url'")
                    continue

                file_id = f"cli_{uuid.uuid4().hex[:6]}"

                profiling = orchestrator.attach_file(
                    file_id=file_id,
                    file_path=volume_path,
                    file_format=file_format,
                    context=semantic_context,
                    domain=selected_domain if mode == "local" else None
                )

                overview = overview_generator.generate(profiling)

                print("\nFile uploaded and profiled successfully.\n")
                print(json.dumps({
                    "dataset_id": file_id,
                    "overview": overview,
                    "profiling": profiling
                }, indent=2))

                current_dataset_id = file_id
                continue

            if not current_dataset_id:
                print("Please upload a file first.")
                continue

            response = orchestrator.run(user_input, current_dataset_id)

            print("\n========== RESPONSE ==========\n")

            if response.get("irrelevant"):
                print("Warning: " + response.get("message", "Question not related to the dataset."))
                continue

            if response.get("error") and not response.get("results"):
                print("Query failed after all retries.")

            print(json.dumps(response, indent=2))

        except Exception as e:
            print("\nERROR:", str(e))


if __name__ == "__main__":
    main()
