from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
import uuid
from config.settings import settings

logger = get_logger(__name__)

app = FastAPI(
    title="INAAS Analytics API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

orchestrator = QueryOrchestrator()

# -------------------------------
# In-Memory Active Dataset
# -------------------------------

ACTIVE_DATASET = {
    "dataset_id": None,
    "file_path": None,
    "file_format": None,
    "profiling": None
}

VOLUME_BASE = settings.databricks_volume_base


class QueryRequest(BaseModel):
    user_input: str


# -------------------------------
# Health
# -------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# =====================================================
# 1️⃣ UPLOAD ONLY (NO INGESTION HERE)
# =====================================================

@app.post("/upload")
async def upload_dataset(
    file: UploadFile = File(None),
    file_path: str = Form(None)
):
    try:
        dataset_id = f"ds_{uuid.uuid4().hex[:6]}"

        # ---------------------------------------
        # OPTION 1: File Upload (streamed)
        # ---------------------------------------
        if file:

            temp_path = f"/tmp/{file.filename}"
            # STREAM FILE IN CHUNKS (prevents 502 memory crash)
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

        ACTIVE_DATASET["dataset_id"] = dataset_id
        ACTIVE_DATASET["file_path"] = volume_path
        ACTIVE_DATASET["file_format"] = detected_format
        ACTIVE_DATASET["profiling"] = None

        return {
            "success": True,
            "dataset_id": dataset_id,
            "file_path": volume_path,
            "message": "Upload successful. Call /start-profiling to generate profiling."
        }

    except Exception as e:
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=str(e))


# =====================================================
# 2️⃣ START PROFILING (Separate Call)
# =====================================================

@app.post("/start-profiling")
def start_profiling():

    if not ACTIVE_DATASET["dataset_id"]:
        raise HTTPException(status_code=400, detail="No dataset uploaded")

    profiling = orchestrator.attach_file(
        file_id=ACTIVE_DATASET["dataset_id"],
        file_path=ACTIVE_DATASET["file_path"],
        file_format=ACTIVE_DATASET["file_format"]
    )

    ACTIVE_DATASET["profiling"] = profiling

    return {
        "success": True,
        "profiling": profiling
    }


# =====================================================
# 3️⃣ GET PROFILING
# =====================================================

@app.get("/profiling")
def get_profiling():

    if not ACTIVE_DATASET["dataset_id"]:
        raise HTTPException(status_code=400, detail="No dataset uploaded")

    return {
        "dataset_id": ACTIVE_DATASET["dataset_id"],
        "profiling": ACTIVE_DATASET["profiling"]
    }


# =====================================================
# 4️⃣ RUN QUERY
# =====================================================

@app.post("/query")
def run_query(request: QueryRequest):

    if not ACTIVE_DATASET["dataset_id"]:
        raise HTTPException(status_code=400, detail="No dataset uploaded")

    try:
        response = orchestrator.run(request.user_input)

        return {
            "success": True,
            "data": response
        }

    except Exception as e:
        logger.exception("Query failed")
        raise HTTPException(status_code=500, detail=str(e))