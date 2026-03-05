from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
from config.settings import settings
from analytics.dataset_overview_generator import DatasetOverviewGenerator
import uuid

logger = get_logger(__name__)

app = FastAPI(
    title="INAAS Analytics API",
    version="2.0.0"
)

# -------------------------------------------------
# CORS (Allow all)
# -------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

orchestrator = QueryOrchestrator()
overview_generator = DatasetOverviewGenerator()
VOLUME_BASE = settings.databricks_volume_base


# =====================================================
# MODELS
# =====================================================

class StartProfilingRequest(BaseModel):
    dataset_id: str
    file_path: str
    file_format: str


class QueryRequest(BaseModel):
    dataset_id: str
    user_input: str


# =====================================================
# HEALTH
# =====================================================

@app.get("/health")
def health():
    return {"status": "ok"}


# =====================================================
# 1️⃣ UPLOAD (Stateless)
# =====================================================

@app.post("/upload")
async def upload_dataset(
    file: UploadFile = File(None),
    file_path: str = Form(None)
):
    try:
        dataset_id = f"ds_{uuid.uuid4().hex[:6]}"

        if file:
            temp_path = f"/tmp/{file.filename}"

            with open(temp_path, "wb") as f:
                while chunk := await file.read(1024 * 1024):
                    f.write(chunk)

            volume_path = orchestrator.executor.upload_to_volume(
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
            "message": "Upload successful. Pass these values to /start-profiling."
        }

    except Exception as e:
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# 2️⃣ START PROFILING (Stateless)
# =====================================================

@app.post("/start-profiling")
def start_profiling(request: StartProfilingRequest):

    try:

        # Step 1 — Run profiling
        profiling = orchestrator.attach_file(
            file_id=request.dataset_id,
            file_path=request.file_path,
            file_format=request.file_format
        )

        # Step 2 — Generate overview using LLM
        overview = overview_generator.generate(profiling)

        # Step 3 — Return response
        return {
            "success": True,
            "dataset_id": request.dataset_id,
            "overview": overview,
            "profiling": profiling
        }

    except Exception as e:
        logger.exception("Profiling failed")
        raise HTTPException(status_code=500, detail=str(e))

# =====================================================
# 3️⃣ RUN QUERY (Stateless)
# =====================================================

@app.post("/query")
def run_query(request: QueryRequest):

    try:
        response = orchestrator.run(
            user_input=request.user_input,
            dataset_id=request.dataset_id
        )

        return {
            "success": True,
            "data": response
        }

    except Exception as e:
        logger.exception("Query failed")
        raise HTTPException(status_code=500, detail=str(e))