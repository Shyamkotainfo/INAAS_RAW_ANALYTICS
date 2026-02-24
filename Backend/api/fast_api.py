from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
import uuid
import os
from config.settings import settings

logger = get_logger(__name__)

app = FastAPI(
    title="INAAS Analytics API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

# Databricks Volume Base Path
VOLUME_BASE = settings.databricks_volume_base

# -------------------------------
# Request Models
# -------------------------------

class UploadRequest(BaseModel):
    file_path: str
    file_format: str


class QueryRequest(BaseModel):
    user_input: str


# -------------------------------
# Health
# -------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------------
# Upload Dataset
# -------------------------------

@app.post("/upload")
async def upload_dataset(
    file: UploadFile = File(None),
    file_path: str = Form(None)):
    try:
        dataset_id = f"ds_{uuid.uuid4().hex[:6]}"

        # ---------------------------------------
        # OPTION 1: File Upload (multipart/form-data)
        # ---------------------------------------
        if file:

            contents = await file.read()
            temp_path = f"/tmp/{file.filename}"

            with open(temp_path, "wb") as f:
                f.write(contents)

            volume_path = orchestrator.executor.upload_to_volume(
                local_path=temp_path,
                volume_base_path=VOLUME_BASE
            )

            detected_format = file.filename.split(".")[-1]

        # ---------------------------------------
        # OPTION 2: JSON Body (file_path + format)
        # ---------------------------------------
        elif file_path and file_format:

            volume_path = file_path
            detected_format = file_format

        else:
            raise HTTPException(
                status_code=400,
                detail="Provide either file upload or file_path + file_format"
            )

        profiling = orchestrator.attach_file(
            file_id=dataset_id,
            file_path=volume_path,
            file_format=detected_format
        )

        ACTIVE_DATASET["dataset_id"] = dataset_id
        ACTIVE_DATASET["file_path"] = volume_path
        ACTIVE_DATASET["file_format"] = detected_format
        ACTIVE_DATASET["profiling"] = profiling

        return {
            "success": True,
            "dataset_id": dataset_id,
            "file_path": volume_path,
            "profiling": profiling
        }

    except Exception as e:
        logger.exception("Upload failed")
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------------
# Get Profiling
# -------------------------------

@app.get("/profiling")
def get_profiling():

    if not ACTIVE_DATASET["dataset_id"]:
        raise HTTPException(status_code=400, detail="No dataset uploaded")

    return {
        "dataset_id": ACTIVE_DATASET["dataset_id"],
        "profiling": ACTIVE_DATASET["profiling"]
    }


# -------------------------------
# Run Query
# -------------------------------

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