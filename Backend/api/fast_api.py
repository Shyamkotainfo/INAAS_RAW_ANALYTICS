from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
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
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

overview_generator = DatasetOverviewGenerator()
VOLUME_BASE = settings.databricks_volume_base
upload_orchestrator = QueryOrchestrator()
dataset_sessions: dict[str, QueryOrchestrator] = {}


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

        semantic_context = None
        selected_domain = None
        if selected_context != "none":
            selected_domain = selected_context
            semantic_context = settings.DOMAIN_WIKI_ROOTS.get(selected_domain)

        session_orchestrator = QueryOrchestrator()
        profiling = session_orchestrator.attach_file(
            file_id=request.dataset_id,
            file_path=request.file_path,
            file_format=request.file_format,
            context=request.semantic_context,
            domain=selected_domain
        )
        dataset_sessions[request.dataset_id] = session_orchestrator

        overview = overview_generator.generate(profiling)

        return {
            "success": True,
            "dataset_id": request.dataset_id,
            "business_context": selected_context,
            "overview": overview,
            "profiling": profiling
        }
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Profiling failed")
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
