# Backend/execution/databricks_executor.py
import requests
import time
import json
from config.settings import settings


class DatabricksExecutor:
    def execute(self, context: dict, pyspark_code: str) -> dict:
        pyspark_code = self._sanitize_code(pyspark_code)

        payload = {
            "run_name": "INAAS Query",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_run_query_script,
                "parameters": [
                    json.dumps({
                        "file_path": context["file_path"],
                        "format": context.get("format", "csv"),
                        "pyspark_code": pyspark_code
                    })
                ]
            }
        }

        resp = requests.post(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            json=payload
        )
        resp.raise_for_status()
        run_id = resp.json()["run_id"]

        while True:
            status = requests.get(
                f"https://{settings.databricks_host}/api/2.1/jobs/runs/get",
                headers={"Authorization": f"Bearer {settings.databricks_token}"},
                params={"run_id": run_id}
            ).json()


            state = status["state"]["life_cycle_state"]
            if state == "TERMINATED":
                result_state = status["state"].get("result_state")
                if result_state != "SUCCESS":
                    out = requests.get(
                        f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
                        headers={"Authorization": f"Bearer {settings.databricks_token}"},
                        params={"run_id": run_id}
                    ).json()

                    stdout = out.get("logs", "")
                    stderr = out.get("error", "")

                    print("\n========== DATABRICKS STDOUT ==========")
                    print(stdout)

                    if stderr:
                        print("\n========== DATABRICKS STDERR ==========")
                        print(stderr)

                    return {
                        "status": "FAILED",
                        "error": "Databricks execution failed — see logs above"
                    }

                break

            time.sleep(2)

        out = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        ).json()

        stdout = out.get("logs", "")
        stderr = out.get("error", "")

        print("\n========== DATABRICKS STDOUT ==========")
        print(stdout)

        if stderr:
            print("\n========== DATABRICKS STDERR ==========")
            print(stderr)

            return {
                "status": "FAILED",
                "error": stderr
            }

        for line in reversed(stdout.splitlines()):
            if line.startswith("INAAS_RESULT:"):
                return {
                    "status": "SUCCESS",
                    "result": json.loads(
                        line.replace("INAAS_RESULT:", "").strip()
                    )
                }

        return {
            "status": "FAILED",
            "error": "INAAS_RESULT not found in Databricks logs"
        }


    def _sanitize_code(self, code: str) -> str:
        forbidden = [

        ]

        clean = []
        for line in code.splitlines():
            line = line.strip()

            if not line:
                continue
            if line.startswith("```"):
                continue
            if line.startswith("import ") or line.startswith("from "):
                continue
            if line.lower().startswith("here"):
                continue

            # HARD BLOCK forbidden ops unless explicitly allowed
            for f in forbidden:
                if f in line:
                    raise RuntimeError(
                        f"LLM generated forbidden operation: {f}"
                    )

            clean.append(line)

        return "\n".join(clean)

