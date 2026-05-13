from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Optional
from analytics.dataset_overview_generator import DatasetOverviewGenerator
from config.settings import settings
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
import json
import threading
import time
import uuid
import os

logger = get_logger(__name__)

app = FastAPI(
    title="INAAS Analytics API",
    version="2.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://dev.dgtn2tpyi79ur.amplifyapp.com",        
        "http://localhost:5173",
        "http://localhost:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

overview_generator = DatasetOverviewGenerator()
VOLUME_BASE = settings.databricks_volume_base
upload_orchestrator = QueryOrchestrator()
dataset_sessions: dict[str, QueryOrchestrator] = {}
profiling_jobs: dict[str, dict[str, Any]] = {}
profiling_jobs_lock = threading.Lock()
PROFILING_JOB_TTL_SECONDS = 60 * 60
PROFILING_STATE_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "profiling_jobs_state.json")
)


class StartProfilingRequest(BaseModel):
    dataset_id: str
    file_path: str
    file_format: str
    semantic_context: Optional[str] = None
    business_context: str | None = None


class QueryRequest(BaseModel):
    dataset_id: str
    user_input: str


def _serialize_job(job: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in job.items()
        if key != "orchestrator"
    }


def _persist_profiling_jobs_locked() -> None:
    os.makedirs(os.path.dirname(PROFILING_STATE_PATH), exist_ok=True)
    serialized = {
        job_id: _serialize_job(job)
        for job_id, job in profiling_jobs.items()
    }
    with open(PROFILING_STATE_PATH, "w", encoding="utf-8") as handle:
        json.dump(serialized, handle)


def _prune_stale_jobs_locked(now: float | None = None) -> None:
    current_time = now or time.time()
    expired_job_ids = [
        job_id
        for job_id, job in profiling_jobs.items()
        if job.get("completed_at")
        and current_time - float(job["completed_at"]) > PROFILING_JOB_TTL_SECONDS
    ]

    for job_id in expired_job_ids:
        profiling_jobs.pop(job_id, None)

    if expired_job_ids:
        _persist_profiling_jobs_locked()


def _get_or_create_orchestrator(job: dict[str, Any]) -> QueryOrchestrator:
    orchestrator = job.get("orchestrator")
    if orchestrator is None:
        orchestrator = QueryOrchestrator()
        active_file_snapshot = job.get("active_file_snapshot")
        if isinstance(active_file_snapshot, dict):
            orchestrator.active_file = active_file_snapshot
        job["orchestrator"] = orchestrator
    return orchestrator


def _build_running_response(
    job_id: str,
    job: dict[str, Any],
    *,
    state_message: str | None = None,
    life_cycle_state: str | None = None,
) -> dict[str, Any]:
    return {
        "success": True,
        "job_id": job_id,
        "run_id": job.get("run_id", job_id),
        "dataset_id": job["dataset_id"],
        "business_context": job.get("business_context"),
        "status": "RUNNING",
        "life_cycle_state": life_cycle_state or job.get("life_cycle_state") or "RUNNING",
        "state_message": state_message or job.get("state_message"),
    }


def _build_failure_response(
    job_id: str,
    job: dict[str, Any],
    *,
    error: str,
    life_cycle_state: str | None = None,
    result_state: str | None = None,
    state_message: str | None = None,
    phase: str | None = None,
) -> dict[str, Any]:
    response = {
        "success": False,
        "job_id": job_id,
        "run_id": job.get("run_id", job_id),
        "dataset_id": job["dataset_id"],
        "business_context": job.get("business_context"),
        "status": "FAILED",
        "life_cycle_state": life_cycle_state,
        "result_state": result_state,
        "state_message": state_message,
        "error": error,
    }
    if phase:
        response["phase"] = phase
    return response


def _restore_profiling_jobs() -> None:
    if not os.path.exists(PROFILING_STATE_PATH):
        return

    try:
        with open(PROFILING_STATE_PATH, "r", encoding="utf-8") as handle:
            stored_jobs = json.load(handle)
    except Exception:
        logger.exception("Failed to restore profiling job state from disk")
        return

    if not isinstance(stored_jobs, dict):
        return

    restored_sessions = 0
    with profiling_jobs_lock:
        for job_id, raw_job in stored_jobs.items():
            if not isinstance(raw_job, dict):
                continue

            job = dict(raw_job)
            job.setdefault("run_id", job_id)
            app_status = job.get("app_status")
            active_file_snapshot = job.get("active_file_snapshot")

            if app_status == "SUCCESS" and isinstance(active_file_snapshot, dict):
                orchestrator = QueryOrchestrator()
                orchestrator.active_file = active_file_snapshot
                job["orchestrator"] = orchestrator
                dataset_id = job.get("dataset_id")
                if isinstance(dataset_id, str):
                    dataset_sessions[dataset_id] = orchestrator
                    restored_sessions += 1
            elif app_status in {"RUNNING", "FINALIZING"}:
                job["orchestrator"] = QueryOrchestrator()

            profiling_jobs[job_id] = job

        _prune_stale_jobs_locked()

    if stored_jobs:
        logger.info(
            "Restored profiling jobs from disk | jobs=%d | sessions=%d",
            len(profiling_jobs),
            restored_sessions,
        )


_restore_profiling_jobs()


@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/upload")
async def upload_dataset(
    file: UploadFile = File(None),
    file_path: str = Form(None)
):
    try:
        dataset_id = f"ds_{uuid.uuid4().hex[:6]}"

        # 🔥 FIX: cross-platform temp directory
        temp_dir = os.path.join(os.getcwd(), "temp_uploads")
        os.makedirs(temp_dir, exist_ok=True)

        if file:
            temp_path = os.path.join(temp_dir, file.filename)

            with open(temp_path, "wb") as handle:
                while chunk := await file.read(1024 * 1024):
                    handle.write(chunk)

            volume_path = upload_orchestrator.executor.upload_to_volume(
                local_path=temp_path,
                volume_base_path=VOLUME_BASE
            )

        elif file_path:
            volume_path = file_path

        else:
            raise HTTPException(
                status_code=400,
                detail="Provide either file upload or file_path"
            )

        detected_format = volume_path.split(".")[-1].lower()

        return {
            "success": True,
            "dataset_id": dataset_id,
            "file_path": volume_path,
            "file_format": detected_format,
            "message": "Upload successful. Pass dataset_id, file_path, and file_format to /start-profiling."
        }

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=str(exc))

@app.post("/start-profiling")
def start_profiling(request: StartProfilingRequest):
    try:
        with profiling_jobs_lock:
            _prune_stale_jobs_locked()

        selected_context = (request.business_context or "none").strip().lower()
        allowed_contexts = {"none", *settings.DOMAIN_WIKI_ROOTS.keys()}
        if selected_context not in allowed_contexts:
            raise HTTPException(
                status_code=400,
                detail=(
                    "business_context must be one of: "
                    + ", ".join(sorted(allowed_contexts))
                )
            )

        selected_domain = None
        if selected_context != "none":
            selected_domain = selected_context

        session_orchestrator = QueryOrchestrator()
        run_id = session_orchestrator.start_attach_file(
            file_id=request.dataset_id,
            file_path=request.file_path,
            file_format=request.file_format,
        )
        with profiling_jobs_lock:
            profiling_jobs[run_id] = {
                "run_id": run_id,
                "dataset_id": request.dataset_id,
                "file_path": request.file_path,
                "file_format": request.file_format,
                "semantic_context": request.semantic_context,
                "business_context": selected_context,
                "selected_domain": selected_domain,
                "orchestrator": session_orchestrator,
                "app_status": "RUNNING",
                "created_at": time.time(),
                "completed_at": None,
                "final_response": None,
                "active_file_snapshot": None,
                "life_cycle_state": "PENDING",
                "state_message": "Profiling job submitted",
            }
            _persist_profiling_jobs_locked()

        return {
            "success": True,
            "job_id": run_id,
            "run_id": run_id,
            "status": "RUNNING",
            "dataset_id": request.dataset_id,
            "business_context": selected_context,
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Profiling failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/profiling-status/{job_id}")
def profiling_status(job_id: str):
    try:
        with profiling_jobs_lock:
            _prune_stale_jobs_locked()
            job = profiling_jobs.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail="Profiling job not found.")

            final_response = job.get("final_response")
            if isinstance(final_response, dict):
                return final_response

            if job.get("app_status") == "FINALIZING":
                return _build_running_response(
                    job_id,
                    job,
                    state_message="Profiling job completed in Databricks. Finalizing results.",
                    life_cycle_state="FINALIZING",
                )

            session_orchestrator = _get_or_create_orchestrator(job)

        status = session_orchestrator.executor.get_job_status(job_id)

        if status["status"] == "RUNNING":
            with profiling_jobs_lock:
                current_job = profiling_jobs.get(job_id)
                if current_job:
                    current_job["app_status"] = "RUNNING"
                    current_job["life_cycle_state"] = status.get("life_cycle_state")
                    current_job["state_message"] = status.get("state_message")

            return _build_running_response(
                job_id,
                job,
                state_message=status.get("state_message"),
                life_cycle_state=status.get("life_cycle_state"),
            )

        if status["status"] == "FAILED":
            failure_detail = status.get("state_message") or status.get("result_state") or "Profiling failed"
            try:
                session_orchestrator.executor.get_job_logs(job_id)
            except Exception as exc:
                failure_detail = str(exc)
            failure_response = _build_failure_response(
                job_id,
                job,
                error=failure_detail,
                life_cycle_state=status.get("life_cycle_state"),
                result_state=status.get("result_state"),
                state_message=status.get("state_message"),
                phase="DATABRICKS",
            )
            with profiling_jobs_lock:
                current_job = profiling_jobs.get(job_id)
                if current_job:
                    current_job["app_status"] = "FAILED"
                    current_job["completed_at"] = time.time()
                    current_job["final_response"] = failure_response
                    current_job["life_cycle_state"] = status.get("life_cycle_state")
                    current_job["state_message"] = status.get("state_message")
                    _persist_profiling_jobs_locked()
            return failure_response

        with profiling_jobs_lock:
            current_job = profiling_jobs.get(job_id)
            if not current_job:
                raise HTTPException(status_code=404, detail="Profiling job not found.")

            final_response = current_job.get("final_response")
            if isinstance(final_response, dict):
                return final_response

            if current_job.get("app_status") == "FINALIZING":
                return _build_running_response(
                    job_id,
                    current_job,
                    state_message="Profiling job completed in Databricks. Finalizing results.",
                    life_cycle_state="FINALIZING",
                )

            current_job["app_status"] = "FINALIZING"
            current_job["life_cycle_state"] = status.get("life_cycle_state") or "FINALIZING"
            current_job["state_message"] = "Databricks job finished. Finalizing profiling results."
            _persist_profiling_jobs_locked()

        try:
            profiling = session_orchestrator.finalize_attach_file(
                file_id=job["dataset_id"],
                file_path=job["file_path"],
                file_format=job["file_format"],
                context=job.get("semantic_context"),
                domain=job.get("selected_domain"),
                run_id=job_id
            )
            dataset_sessions[job["dataset_id"]] = session_orchestrator
            overview = overview_generator.generate(profiling)
            success_response = {
                "success": True,
                "job_id": job_id,
                "run_id": job.get("run_id", job_id),
                "dataset_id": job["dataset_id"],
                "business_context": job.get("business_context"),
                "status": "SUCCESS",
                "overview": overview,
                "profiling": profiling
            }
            logger.info("Profiling completed | job_id=%s | dataset_id=%s", job_id, job["dataset_id"])
        except Exception as exc:
            logger.exception("Profiling finalization failed | job_id=%s", job_id)
            failure_response = _build_failure_response(
                job_id,
                job,
                error=str(exc),
                life_cycle_state="FINALIZING",
                result_state=status.get("result_state"),
                state_message="Profiling finalization failed after Databricks completion.",
                phase="FINALIZATION",
            )
            with profiling_jobs_lock:
                current_job = profiling_jobs.get(job_id)
                if current_job:
                    current_job["app_status"] = "FAILED"
                    current_job["completed_at"] = time.time()
                    current_job["final_response"] = failure_response
                    current_job["state_message"] = failure_response["state_message"]
                    _persist_profiling_jobs_locked()
            return failure_response

        with profiling_jobs_lock:
            current_job = profiling_jobs.get(job_id)
            if current_job:
                current_job["app_status"] = "SUCCESS"
                current_job["completed_at"] = time.time()
                current_job["active_file_snapshot"] = dict(session_orchestrator.active_file or {})
                current_job["final_response"] = success_response
                current_job["state_message"] = "Profiling completed successfully."
                _persist_profiling_jobs_locked()
        return success_response
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Profiling status check failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/query")
def run_query(request: QueryRequest):
    try:
        session_orchestrator = dataset_sessions.get(request.dataset_id)
        if not session_orchestrator:
            raise HTTPException(
                status_code=404,
                detail="Dataset session not found. Call /start-profiling first for this dataset_id."
            )

        response = session_orchestrator.run(
            user_input=request.user_input,
            dataset_id=request.dataset_id
        )

        is_success = not response.get("error") and not response.get("irrelevant")
        return {
            "success": is_success,
            "data": response
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Query failed with unexpected error")
        raise HTTPException(
            status_code=500,
            detail={
                "error": True,
                "reason": str(exc),
                "message": "Query failed with unexpected error",
                "error_type": type(exc).__name__,
            },
        )
