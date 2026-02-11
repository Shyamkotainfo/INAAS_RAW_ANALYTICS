from core.query_orchestrator import QueryOrchestrator
import json


def main():
    print("\n" + "=" * 60)
    print("INAAS - RAW Mode")
    print("=" * 60)
    print("Ask questions about your data. Type 'exit' to stop.")
    print("=" * 60)

    orchestrator = QueryOrchestrator()

    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in {"exit", "quit", "q"}:
                print("Goodbye!")
                break

            response = orchestrator.run(user_input)

            print("\n========== RESPONSE ==========\n")
            print(json.dumps(response, indent=2))

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break

        except Exception as e:
            print("\nERROR:", str(e))
