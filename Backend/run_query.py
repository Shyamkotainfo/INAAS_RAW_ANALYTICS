# Backend/run_query.py

from orchestrator.query_orchestrator import QueryOrchestrator

def main():
    question = input("Ask your question: ")

    orchestrator = QueryOrchestrator()
    response = orchestrator.run(question)

    print("\n--- Generated PySpark Code ---")
    print(response["pyspark_code"])

    print("\n--- Result Rows ---")
    for row in response["result"]:
        print(row)

    # print("\n--- Summary ---")
    # print(response["summary"])


if __name__ == "__main__":
    main()
