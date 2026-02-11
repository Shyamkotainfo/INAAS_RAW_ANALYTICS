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

            print("\n--- Generated PySpark Code ---\n")
            print(response["pyspark_code"])

            execution = response["execution"]

            if execution["status"] != "SUCCESS":
                print("\n--- Execution Failed ---")
                print(execution.get("error"))
                continue

            print("\n--- Result ---")
            print("Columns:", execution["result"]["columns"])
            for row in execution["result"]["rows"]:
                print(row)

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break

        except Exception as e:
            # Keep chat alive
            print("\nERROR:", str(e))


if __name__ == "__main__":
    main()
