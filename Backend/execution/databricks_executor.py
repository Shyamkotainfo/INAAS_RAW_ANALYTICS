import requests
import time
import json
import os
import base64
from typing import Any
from config.settings import settings
from pyspark_utils.code_validator import validate_pyspark_code
from logger.logger import get_logger

logger = get_logger(__name__)


class DatabricksExecutor:

    def __init__(self):
        self._backend_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    def _validate_job_runtime_config(self):
        if not settings.databricks_cluster_id:
            raise RuntimeError(
                "DATABRICKS_CLUSTER_ID is not configured. Add your Databricks compute cluster id "
                "to Backend/.env before running profiling or queries."
            )

        if not settings.databricks_ingest_script or not settings.databricks_run_query_script:
            raise RuntimeError(
                "Databricks job script paths are not configured. Check "
                "DATABRICKS_INGEST_SCRIPT and DATABRICKS_RUN_QUERY_SCRIPT."
            )

    # =====================================================
    # INGEST + PROFILE
    # =====================================================
    def submit_ingest_and_profile(self, file_id: str, file_path: str, file_format: str) -> str:
        self._ensure_remote_script(
            settings.databricks_ingest_script,
            os.path.join(self._backend_root, "databricks_scripts", "databricks", "ingest_and_profile.py")
        )
        self._validate_job_runtime_config()

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

        return self.submit_job(payload)

    def finalize_ingest_and_profile(self, run_id: str):
        stdout = self.get_job_logs(run_id)
        schema = self._extract_schema(stdout)
        profiling = self._extract_profiling(stdout)

        return {
            "status": "SUCCESS",
            "schema": schema,
            "profiling": profiling
        }

    def ingest_and_profile(self, file_id: str, file_path: str, file_format: str):
        run_id = self.submit_ingest_and_profile(file_id, file_path, file_format)
        self.wait_for_job_completion(run_id)
        return self.finalize_ingest_and_profile(run_id)

    def read_volume_text(self, volume_path: str) -> str:
        normalized_path = volume_path.strip()
        if normalized_path.startswith("dbfs:/Volumes/"):
            normalized_path = normalized_path.replace("dbfs:", "", 1)

        if not normalized_path.startswith("/Volumes/"):
            raise RuntimeError(
                f"Semantic context must be a Databricks Volume path. Received: {volume_path}"
            )

        resp = requests.get(
            f"https://{settings.databricks_host}/api/2.0/fs/files{normalized_path}",
            headers={"Authorization": f"Bearer {settings.databricks_token}"}
        )
        resp.raise_for_status()

        text = resp.text
        if not text.strip():
            raise RuntimeError(f"Semantic context file is empty: {normalized_path}")

        logger.info(
            "Read semantic context from Databricks Volume | path=%s | chars=%d",
            normalized_path,
            len(text)
        )

        return text

    # =====================================================
    # UPLOAD TO DATABRICKS
    # =====================================================
    def upload_to_volume(self, local_path: str, volume_base_path: str):
        import os
        import uuid

        if not volume_base_path:
            raise RuntimeError(
                "Databricks upload path is not configured. Set DATABRICKS_VOLUME_BASE "
                "or use the default raw_data volume path."
            )

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
        validate_pyspark_code(
            pyspark_code,
            available_columns=[c["name"] for c in context.get("columns", [])]
        )
        self._ensure_remote_script(
            settings.databricks_run_query_script,
            os.path.join(self._backend_root, "databricks_scripts", "databricks", "run_query.py")
        )
        self._validate_job_runtime_config()

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

    def _ensure_remote_script(self, remote_path: str, local_path: str):
        if not remote_path:
            return

        if not os.path.exists(local_path):
            raise RuntimeError(f"Local Databricks script not found: {local_path}")

        if remote_path.startswith("dbfs:/") and not remote_path.startswith("dbfs:/Volumes/"):
            with open(local_path, "rb") as f:
                encoded = base64.b64encode(f.read()).decode("utf-8")

            resp = requests.post(
                f"https://{settings.databricks_host}/api/2.0/dbfs/put",
                headers={
                    "Authorization": f"Bearer {settings.databricks_token}",
                    "Content-Type": "application/json"
                },
                json={
                    "path": remote_path.replace("dbfs:", ""),
                    "contents": encoded,
                    "overwrite": True
                }
            )
            resp.raise_for_status()
            return

        volume_path = remote_path.replace("dbfs:", "", 1) if remote_path.startswith("dbfs:/Volumes/") else remote_path
        if volume_path.startswith("/Volumes/"):
            with open(local_path, "rb") as f:
                file_bytes = f.read()

            resp = requests.put(
                f"https://{settings.databricks_host}/api/2.0/fs/files{volume_path}",
                headers={
                    "Authorization": f"Bearer {settings.databricks_token}",
                    "Content-Type": "application/octet-stream"
                },
                data=file_bytes,
                params={"overwrite": "true"}
            )
            resp.raise_for_status()

    def _submit_and_get_logs(self, payload):
        run_id = self.submit_job(payload)
        self.wait_for_job_completion(run_id)
        return self.get_job_logs(run_id)

    def submit_job(self, payload: dict[str, Any]) -> str:
        submit_start = time.time()

        resp = requests.post(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/submit",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            json=payload
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            response_body = resp.text.strip()
            if response_body:
                raise RuntimeError(
                    f"Databricks job submission failed ({resp.status_code}): {response_body}"
                ) from exc
            raise

        run_id = str(resp.json()["run_id"])
        elapsed = time.time() - submit_start
        print("Elapsed:", time.time() - submit_start)
        logger.info("Databricks job submit complete | run_id=%s | elapsed=%.2fs", run_id, elapsed)
        return run_id

    def get_job_status(self, run_id: str) -> dict[str, Any]:
        resp = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        )
        resp.raise_for_status()

        status = resp.json()
        state = status.get("state", {})
        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
        result_state = state.get("result_state")
        state_message = state.get("state_message")

        if life_cycle_state == "TERMINATED":
            normalized_status = "SUCCESS" if result_state == "SUCCESS" else "FAILED"
        elif life_cycle_state in {"INTERNAL_ERROR", "SKIPPED"}:
            normalized_status = "FAILED"
        else:
            normalized_status = "RUNNING"

        logger.info(
            "Checking run status | run_id=%s | life_cycle_state=%s | result_state=%s | normalized_status=%s",
            run_id,
            life_cycle_state,
            result_state,
            normalized_status,
        )

        return {
            "run_id": str(run_id),
            "status": normalized_status,
            "life_cycle_state": life_cycle_state,
            "result_state": result_state,
            "state_message": state_message,
            "raw": status,
        }

    def wait_for_job_completion(self, run_id: str, poll_interval_seconds: int = 3) -> dict[str, Any]:
        poll_start = time.time()

        while True:
            status = self.get_job_status(run_id)
            if status["status"] == "SUCCESS":
                elapsed = time.time() - poll_start
                print("Elapsed:", time.time() - poll_start)
                logger.info("Databricks polling complete | run_id=%s | status=%s | elapsed=%.2fs", run_id, status["status"], elapsed)
                return status

            if status["status"] == "FAILED":
                elapsed = time.time() - poll_start
                print("Elapsed:", time.time() - poll_start)
                logger.info("Databricks polling complete | run_id=%s | status=%s | elapsed=%.2fs", run_id, status["status"], elapsed)
                message = status.get("state_message") or status.get("result_state") or status.get("life_cycle_state")
                raise RuntimeError(f"Databricks job failed: {message}")

            time.sleep(poll_interval_seconds)

    def get_job_logs(self, run_id: str) -> str:
        logs_start = time.time()

        output_resp = requests.get(
            f"https://{settings.databricks_host}/api/2.1/jobs/runs/get-output",
            headers={"Authorization": f"Bearer {settings.databricks_token}"},
            params={"run_id": run_id}
        )
        output_resp.raise_for_status()
        output = output_resp.json()

        stdout = output.get("logs", "")
        stderr = output.get("error", "")

        elapsed = time.time() - logs_start
        print("Elapsed:", time.time() - logs_start)
        logger.info("Databricks log fetch complete | run_id=%s | elapsed=%.2fs", run_id, elapsed)

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
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "Databricks schema output was truncated or malformed. "
                        "Reduce profiling payload or check job logs."
                    ) from exc

        raise RuntimeError("Schema JSON not found in logs")

    # -----------------------------------------------------

    def _extract_profiling(self, stdout: str):

        for line in stdout.splitlines():
            if line.startswith("INAAS_PROFILING="):
                json_part = line.replace("INAAS_PROFILING=", "").strip()
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "Databricks profiling output was truncated or malformed. "
                        "Reduce profiling payload or check job logs."
                    ) from exc

        raise RuntimeError("Profiling JSON not found in logs")

    # -----------------------------------------------------

    def _extract_query_result(self, stdout: str):
        chunk_prefix = "INAAS_RESULT_CHUNK:"
        if "INAAS_RESULT_BEGIN" in stdout and "INAAS_RESULT_END" in stdout:
            encoded_chunks = []
            for line in stdout.splitlines():
                if line.startswith(chunk_prefix):
                    encoded_chunks.append(line.replace(chunk_prefix, "", 1).strip())

            if not encoded_chunks:
                raise RuntimeError("Query result chunks were not found in logs")

            try:
                decoded = base64.b64decode("".join(encoded_chunks)).decode("utf-8")
                return json.loads(decoded)
            except Exception as exc:
                raise RuntimeError(
                    "Chunked query result was truncated or malformed. Check Databricks job logs."
                ) from exc

        for line in stdout.splitlines():
            if line.startswith("INAAS_RESULT:"):
                json_part = line.replace("INAAS_RESULT:", "").strip()
                try:
                    return json.loads(json_part)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "Query result JSON was truncated or malformed. Check Databricks job logs."
                    ) from exc

        raise RuntimeError("Query result not found in logs")
