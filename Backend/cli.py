from core.query_orchestrator import QueryOrchestrator
from analytics.dataset_overview_generator import DatasetOverviewGenerator
from config.settings import settings
import json
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
                parts = user_input.split(" ", 2)

                if len(parts) < 3:
                    print("Invalid command.")
                    print("Use: upload local <file_path> OR upload url <volume_path>")
                    continue

                mode = parts[1].lower()
                path = parts[2].strip()

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

                # -----------------------------
                # VOLUME PATH (already uploaded)
                # -----------------------------
                elif mode == "url":
                    volume_path = path
                    file_format = detect_format(volume_path)
                    semantic_context = None

                else:
                    print("Invalid mode. Use 'local' or 'url'")
                    continue

                # -----------------------------
                # PROFILING
                # -----------------------------
                profiling = orchestrator.attach_file(
                    file_id=file_id,
                    file_path=volume_path,
                    file_format=file_format,
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
                print("⚠️ " + response.get("message", "Question not related to dataset"))
                continue

            if response.get("error") and not response.get("results"):
                print("❌ Query failed after all retries.")

            print(json.dumps(response, indent=2))

        except Exception as e:
            print("\n❌ ERROR:", str(e))


if __name__ == "__main__":
    main()