# Backend/test_query.py
"""
Utility script for rapid NL testing against previously uploaded datasets.
Avoids the full 'upload and profile' cycle by loading context from S3.
"""

import sys
import json
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger

logger = get_logger(__name__)

def run_test_session(dataset_id: str, file_path: str, file_format: str):
    orchestrator = QueryOrchestrator()
    
    print(f"\nRestoring context for: {dataset_id}...")
    try:
        orchestrator.load_from_s3(dataset_id, file_path, file_format)
    except Exception as e:
        print(f"FAILED: {e}")
        return

    print("Context restored. Entering NL query mode (type 'exit' to stop).")
    
    while True:
        question = input("\n[NL Query] > ").strip()
        if not question or question.lower() in ("exit", "quit", "q"):
            break
            
        try:
            response = orchestrator.run(question, dataset_id)
            
            print("\n" + "="*80)
            print(f"ROUTE: {response.get('route')}")
            print("-" * 80)
            
            if response.get("answer"):
                print(response["answer"])
            else:
                # ADHOC result
                print(json.dumps(response, indent=2))
                
            print("="*80)
        except Exception as e:
            print(f"Query Error: {e}")

if __name__ == "__main__":
    # You can pass your existing dataset_id here for convenience
    # Example: python test_query.py cli_a1b2c3 "s3://..." "csv"
    if len(sys.argv) < 4:
        print("Usage: python test_query.py <dataset_id> <file_path> <file_format>")
        sys.exit(1)
        
    ds_id = sys.argv[1]
    f_path = sys.argv[2]
    f_fmt = sys.argv[3]
    
    run_test_session(ds_id, f_path, f_fmt)
