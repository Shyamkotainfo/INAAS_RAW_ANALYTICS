import requests
import time
import json
from config.settings import settings


class DatabricksExecutor:

    # =====================================================
    # INGEST + PROFILE
    # =====================================================
    def ingest_and_profile(self, file_id: str, file_path: str, file_format: str):

        job_args = {
            "file_id": file_id,
            "file_path": file_path,
            "format": file_format
        }

        payload = {
            "run_name": "INAAS Ingest + Profile",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_ingest_script,
                "parameters": [json.dumps(job_args)]
            }
        }

        stdout = self._submit_and_get_logs(payload)

        schema = self._extract_schema(stdout)
        profiling = self._extract_profiling(stdout)

        return {
            "status": "SUCCESS",
            "schema": schema,
            "profiling": profiling
        }

    # =====================================================
    # UPLOAD TO DATABRICKS
    # =====================================================
    def upload_to_volume(self, local_path: str, volume_base_path: str):
        import os
        import uuid

        unique_name = f"{uuid.uuid4().hex}_{os.path.basename(local_path)}"
        target_path = f"{volume_base_path.rstrip('/')}/{unique_name}"

        with open(local_path, "rb") as f:
            file_bytes = f.read()

        url = (
            f"https://{settings.databricks_host}"
            f"/api/2.0/fs/files{target_path}"
        )

        resp = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {settings.databricks_token}",
                "Content-Type": "application/octet-stream"
            },
            data=file_bytes
        )

        resp.raise_for_status()

        return target_path

    # =====================================================
    # QUERY EXECUTION
    # =====================================================
    def execute_query(self, context: dict, pyspark_code: str):

        job_args = {
            "file_path": context["file_path"],
            "format": context.get("format", "csv"),
            "pyspark_code": pyspark_code
        }

        payload = {
            "run_name": "INAAS Query",
            "existing_cluster_id": settings.databricks_cluster_id,
            "spark_python_task": {
                "python_file": settings.databricks_run_query_script,
                "parameters": [json.dumps(job_args)]
            }
        }

        # Capture stdout without raising — return structured result so
        # the orchestrator retry loop can handle failures gracefully.
        try:
            stdout = self._submit_and_get_logs(payload)
        except RuntimeError as e:
            return {
                "status": "FAILED",
                "result": None,
                "error": str(e),
                "raw_stdout": ""
            }

        # Check for an execution error marker in the logs
        if "INAAS_EXECUTION_ERROR" in stdout:
            error_lines = []
            capture = False
            for line in stdout.splitlines():
                if "INAAS_EXECUTION_ERROR" in line:
                    capture = True
                    continue
                if capture:
                    error_lines.append(line)
            return {
                "status": "FAILED",
                "result": None,
                "error": "\n".join(error_lines).strip() or "Unknown execution error",
                "raw_stdout": stdout
            }

        try:
            result = self._extract_query_result(stdout)
        except RuntimeError as e:
            return {
                "status": "FAILED",
                "result": None,
                "error": str(e),
                "raw_stdout": stdout
            }

        return {
            "status": "SUCCESS",
            "result": result,
            "error": None,
            "raw_stdout": stdout
        }

    # =====================================================
    # INTERNAL HELPERS
    # =====================================================

    def _submit_and_get_logs(self, payload):

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
                break

            if state in {"INTERNAL_ERROR", "SKIPPED"}:
                raise RuntimeError(f"Databricks job failed: {state}")

            time.sleep(3)

        output = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        ).json()

        stdout = output.get("logs", "")
        stderr = output.get("error", "")

        print("\n========== RAW DATABRICKS STDOUT ==========\n")
        print(stdout)

        if stderr:
            print("\n========== RAW DATABRICKS STDERR ==========\n")
            print(stderr)

        print("\n============================================\n")

        if stderr:
            raise RuntimeError(f"Databricks job failed:\n{stderr}")

        if not stdout:
            raise RuntimeError("No logs returned from Databricks")

        return stdout


    # -----------------------------------------------------

    def _extract_schema(self, stdout: str):

        for line in stdout.splitlines():
            if line.startswith("INAAS_SCHEMA_JSON="):
                json_part = line.replace("INAAS_SCHEMA_JSON=", "").strip()
                return json.loads(json_part)

        raise RuntimeError("Schema JSON not found in logs")

    # -----------------------------------------------------

    def _extract_profiling(self, stdout: str):

        for line in stdout.splitlines():
            if line.startswith("INAAS_PROFILING="):
                json_part = line.replace("INAAS_PROFILING=", "").strip()
                return json.loads(json_part)

        raise RuntimeError("Profiling JSON not found in logs")

    # -----------------------------------------------------

    def _extract_query_result(self, stdout: str):

        for line in stdout.splitlines():
            if line.startswith("INAAS_RESULT:"):
                json_part = line.replace("INAAS_RESULT:", "").strip()
                return json.loads(json_part)

        raise RuntimeError("Query result not found in logs")
