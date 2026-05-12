from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Any, Optional
from analytics.dataset_overview_generator import DatasetOverviewGenerator
from config.settings import settings
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
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


class StartProfilingRequest(BaseModel):
    dataset_id: str
    file_path: str
    file_format: str
    semantic_context: Optional[str] = None
    business_context: str | None = None


class QueryRequest(BaseModel):
    dataset_id: str
    user_input: str


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
        profiling_jobs[run_id] = {
            "dataset_id": request.dataset_id,
            "file_path": request.file_path,
            "file_format": request.file_format,
            "semantic_context": request.semantic_context,
            "business_context": selected_context,
            "selected_domain": selected_domain,
            "orchestrator": session_orchestrator,
        }

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
        job = profiling_jobs.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Profiling job not found.")

        session_orchestrator = job["orchestrator"]
        status = session_orchestrator.executor.get_job_status(job_id)

        if status["status"] == "RUNNING":
            return {
                "success": True,
                "job_id": job_id,
                "dataset_id": job["dataset_id"],
                "status": "RUNNING",
                "life_cycle_state": status.get("life_cycle_state"),
                "state_message": status.get("state_message"),
            }

        if status["status"] == "FAILED":
            failure_detail = status.get("state_message") or status.get("result_state") or "Profiling failed"
            try:
                session_orchestrator.executor.get_job_logs(job_id)
            except Exception as exc:
                failure_detail = str(exc)

            profiling_jobs.pop(job_id, None)
            return {
                "success": False,
                "job_id": job_id,
                "dataset_id": job["dataset_id"],
                "status": "FAILED",
                "life_cycle_state": status.get("life_cycle_state"),
                "result_state": status.get("result_state"),
                "state_message": status.get("state_message"),
                "error": failure_detail,
            }

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

        profiling_jobs.pop(job_id, None)
        return {
            "success": True,
            "job_id": job_id,
            "dataset_id": job["dataset_id"],
            "business_context": job.get("business_context"),
            "status": "SUCCESS",
            "overview": overview,
            "profiling": profiling
        }
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
        raise HTTPException(status_code=500, detail=str(exc))
