from core.query_orchestrator import QueryOrchestrator
import json
import uuid
from config.settings import settings


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

    # 🔥 Define your volume base here
    VOLUME_BASE = settings.databricks_volume_base

    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in {"exit", "quit", "q"}:
                print("Goodbye!")
                break

            # ----------------------------------
            # Upload Command
            # ----------------------------------
            if user_input.startswith("upload "):
                parts = user_input.split(" ", 2)

                if len(parts) < 3:
                    print("Invalid command. Use:")
                    print("  upload local <file_path>")
                    print("  upload url <volume_path>")
                    continue

                mode = parts[1].lower()
                path = parts[2].strip()

                file_id = f"cli_{uuid.uuid4().hex[:6]}"

                # ------------------------------------------
                # OPTION 1: Local Upload → Volume
                # ------------------------------------------
                if mode == "local":

                    file_format = detect_format(path)

                    print("Uploading file to Databricks Volume...")

                    volume_path = orchestrator.executor.upload_to_volume(
                        local_path=path,
                        volume_base_path=VOLUME_BASE
                    )

                    print(f"Uploaded to: {volume_path}")

                # ------------------------------------------
                # OPTION 2: Direct Volume Path
                # ------------------------------------------
                elif mode == "url":

                    volume_path = path
                    file_format = detect_format(volume_path)

                else:
                    print("Invalid mode. Use 'local' or 'url'")
                    continue

                # 🔥 Common Attach Logic
                profiling = orchestrator.attach_file(
                    file_id=file_id,
                    file_path=volume_path,
                    file_format=file_format
                )

                print("\nFile uploaded and profiled successfully.")
                print(json.dumps(profiling, indent=2))
                continue

            # ----------------------------------
            # Normal Question
            # ----------------------------------
            response = orchestrator.run(user_input)

            print("\n========== RESPONSE ==========\n")
            print(json.dumps(response, indent=2))

        except Exception as e:
            print("\nERROR:", str(e))