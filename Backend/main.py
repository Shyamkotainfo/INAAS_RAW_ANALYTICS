# Backend/main.py

from core.query_orchestrator import QueryOrchestrator


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

            if "sql" in response:
                print("\n--- Generated SQL ---\n")
                print(response["sql"])

                print("\n--- Result ---")
                result = response["result"]
                print("Columns:", result["columns"])
                for row in result["rows"]:
                    print(row)
            else:
                print("\n--- Generated PySpark Code ---\n")
                print(response["pyspark_code"])

                print("\n--- Execution Status ---")
                for k, v in response["execution"].items():
                    print(f"{k}: {v}")

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print("\nERROR:", str(e))


if __name__ == "__main__":
    main()
