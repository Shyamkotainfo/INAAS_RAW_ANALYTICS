"""
Main entry point for IAAS Bot
Supports both CLI and API modes
"""
from core.query_orchestrator import QueryOrchestrator
import json

import sys

#  Add this right after importing sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import os
import argparse

# Add the app directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))


def main():
    """Main entry point with argument parsing"""
    parser = argparse.ArgumentParser(
        description="INAAS Bot - Insight as a Service For Raw Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                    # Start CLI mode
  python main.py --mode cli         # Start CLI mode explicitly
  python main.py --mode api         # Start API server
  python main.py --mode api --port 8080  # Start API on custom port
  python main.py --mode ingest_schema
        """
    )

    parser.add_argument(
        "--mode",
        choices=["cli", "api", "ingest_schema"],
        default="cli",
        help="Mode to run the application (default: cli)"
    )

    parser.add_argument(
        "--port",
        type=int,
        default=8081,
        help="Port for API mode (default: 8000)"
    )

    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host for API mode (default: 0.0.0.0)"
    )

    parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for API mode (development only)"
    )


    args = parser.parse_args()

    if args.mode == "cli":
        run_cli()
    elif args.mode == "api":
        run_api(host=args.host, port=args.port, reload=args.reload)
    elif args.mode == "ingest_schema":
        run_schema_ingestion()


def run_cli():
    """Run CLI mode"""
    try:
        from cli import main as cli_main
        cli_main()
    except ImportError as e:
        print(f" Failed to import CLI module: {e}")
        print("Make sure all dependencies are installed.")
        sys.exit(1)
    except Exception as e:
        print(f" CLI error: {e}")
        sys.exit(1)


def run_api(host: str = "0.0.0.0", port: int = 8000, reload: bool = False):
    """Run API mode"""
    try:
        import uvicorn
        from api.fast_api import app

        print(f" Starting IAAS Bot API server...")
        print(f" Host: {host}")
        print(f" Port: {port}")
        print(f" Reload: {reload}")
        print(f" Docs: http://{host}:{port}/docs")
        print("=" * 50)

        uvicorn.run(
            "api.fast_api:app",
            host=host,
            port=port,
            reload=reload,
            log_level="info"
        )
    except ImportError as e:
        print(f" Failed to import API module: {e}")
        print("Make sure all dependencies are installed.")
        sys.exit(1)
    except Exception as e:
        print(f" API error: {e}")
        sys.exit(1)


def run_schema_ingestion(dw_type: str | None = None):
    """Run schema ingestion and KB indexing flow"""
    try:
        from ingestion.run_ingestion import main as ingestion_main

        print("Starting schema ingestion...")
        ingestion_main()
        print("Schema ingestion completed successfully.")
    except Exception as e:
        print(f"Schema ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

