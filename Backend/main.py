# Backend/main.py

from core.query_orchestrator import QueryOrchestrator

def main():
    print("\n" + "=" * 60)
    print(f"INAAS - RAW Mode")
    print("=" * 60)
    print("Ask questions about your data. Type 'exit' to stop.")
    print("=" * 60)

    # ---------------- MAIN LOOP ----------------
    while True:
        try:
            user_input = input("\n> ").strip()

            if not user_input:
                continue

            if user_input.lower() in ["exit", "quit", "q"]:
                print(" Goodbye!")
                break

            orchestrator = QueryOrchestrator()
            response = orchestrator.run(user_input)

            print("\n--- Generated PySpark Code ---")
            print(response["pyspark_code"])

            print("\n--- Result Rows ---")
            for row in response["result"]:
                print(row)

            # print("\n--- Summary ---")
            # print(response["summary"])

        except KeyboardInterrupt:
            print("\n Goodbye!")
            break

if __name__ == "__main__":
    main()
