from core.query_orchestrator import QueryOrchestrator
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
    print("  upload <volume_path>")
    print("  ask natural language question")
    print("  exit")
    print("=" * 60)

    orchestrator = QueryOrchestrator()

    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in {"exit", "quit"}:
                print("Goodbye!")
                break

            # ----------------------------------
            # Upload Command
            # ----------------------------------
            if user_input.startswith("upload "):
                volume_path = user_input.replace("upload ", "").strip()

                file_format = detect_format(volume_path)
                file_id = f"cli_{uuid.uuid4().hex[:6]}"

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
