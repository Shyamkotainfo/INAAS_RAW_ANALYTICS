from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger
import uuid

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
def upload_dataset(request: UploadRequest):
    try:
        dataset_id = f"ds_{uuid.uuid4().hex[:6]}"

        profiling = orchestrator.attach_file(
            file_id=dataset_id,
            file_path=request.file_path,
            file_format=request.file_format
        )

        ACTIVE_DATASET["dataset_id"] = dataset_id
        ACTIVE_DATASET["file_path"] = request.file_path
        ACTIVE_DATASET["file_format"] = request.file_format
        ACTIVE_DATASET["profiling"] = profiling

        return {
            "success": True,
            "dataset_id": dataset_id,
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
