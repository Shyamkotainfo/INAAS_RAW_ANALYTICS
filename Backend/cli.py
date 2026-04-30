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
    print("  upload local <csv_path> <context_json_path>")
    print("  upload local <local_file_path> [optional_context_json_path]")
    print("  upload url <volume_path>")
    print("  ask natural language question")
    print("  exit")
    print("=" * 60)

    orchestrator = QueryOrchestrator()
    overview_generator = DatasetOverviewGenerator()

    VOLUME_BASE = settings.databricks_volume_base
    current_dataset_id = None

    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in {"exit", "quit", "q"}:
                print("Goodbye!")
                break

            # ==================================================
            # UPLOAD COMMAND
            # ==================================================
            if user_input.startswith("upload "):

                try:
                    parts = shlex.split(user_input, posix=False)
                except ValueError as e:
                    print(f"Invalid command syntax: {e}")
                    print('Wrap paths containing spaces in double quotes.')
                    continue

                if len(parts) < 3:
                    print("Invalid command.")
                    print("Use:")
                    print("  upload local <csv_path> <context_json_path>")
                    print("  upload local <file_path> [optional_context_json_path]")
                    print("  upload url <volume_path>")
                parts = user_input.split(" ", 2)

                if len(parts) < 3:
                    print("Invalid command.")
                    print("Use: upload local <file_path> OR upload url <volume_path>")
                    continue

                mode = parts[1].lower()
                context_str = None

                if mode == "local":
                    if len(parts) not in {3, 4}:
                        print("Invalid local upload command.")
                        print("Use:")
                        print("  upload local <csv_path> <context_json_path>")
                        print("  upload local <file_path> [optional_context_json_path]")
                        print('Wrap paths containing spaces in double quotes.')
                        continue

                    path = parts[2].strip()
                    context_path = parts[3].strip() if len(parts) == 4 else None

                    if context_path:
                        try:
                            with open(context_path, "r", encoding="utf-8") as f:
                                context_str = f.read()
                            print(f"Loaded semantic context from {context_path}")
                        except Exception as e:
                            print(f"Failed to load context file: {e}")
                            continue

                    try:
                        with open(path, "rb"):
                            pass
                    except Exception as e:
                        print(f"Failed to read local file: {e}")
                        continue

                elif mode == "url":
                    if len(parts) != 3:
                        print("Invalid url upload command.")
                        print("Use: upload url <volume_path>")
                        continue
                    path = parts[2].strip()
                else:
                    print("Invalid mode. Use 'local' or 'url'")
                    continue

                file_id = f"cli_{uuid.uuid4().hex[:6]}"

                # -----------------------------
                # LOCAL FILE UPLOAD
                # -----------------------------
                if mode == "local":
                    file_format = detect_format(path)

                    print("\nUploading file to Databricks Volume...")

                    volume_path = orchestrator.executor.upload_to_volume(
                        local_path=path,
                        volume_base_path=VOLUME_BASE
                    )

                    print(f"Uploaded to: {volume_path}")

                    # 🔥 SAME AS FASTAPI LOGIC
                    print("\nSelect Business Context:")
                    print("1. HR")
                    print("2. None")

                    choice = input("Enter choice: ").strip()

                    domain_map = {
                        "1": "hr",
                        "2": None
                    }

                    selected_domain = domain_map.get(choice)

                    if choice not in domain_map:
                        print("Invalid choice. Defaulting to no context.")

                    semantic_context = None
                    if selected_domain:
                        semantic_context = settings.DOMAIN_WIKI_ROOTS.get(selected_domain)

                elif mode == "url":
                    volume_path = path
                    file_format = detect_format(volume_path)
                    semantic_context = None


                # -----------------------------
                # PROFILING
                # -----------------------------
                profiling = orchestrator.attach_file(
                    file_id=file_id,
                    file_path=volume_path,
                    file_format=file_format,
                    context=context_str if mode == "local" else None
                    context=semantic_context
                )

                overview = overview_generator.generate(profiling)

                print("\n✅ File uploaded and profiled successfully.\n")

                print(json.dumps({
                    "dataset_id": file_id,
                    "overview": overview,
                    "profiling": profiling
                }, indent=2))

                current_dataset_id = file_id
                continue

            # ==================================================
            # QUERY
            # ==================================================
            if not current_dataset_id:
                print("⚠️  Please upload a file first.")
                continue

            response = orchestrator.run(user_input, current_dataset_id)

            print("\n========== RESPONSE ==========\n")

            if response.get("irrelevant"):
                print("Warning: " + response.get("message", "Question not related to the dataset."))
                continue

            if response.get("error") and not response.get("results"):
                print("Query failed after all retries.")
                print("⚠️ " + response.get("message", "Question not related to dataset"))
                continue

            if response.get("error") and not response.get("results"):
                print("❌ Query failed after all retries.")

            print(json.dumps(response, indent=2))

        except Exception as e:
            print("\n❌ ERROR:", str(e))


if __name__ == "__main__":
    main()