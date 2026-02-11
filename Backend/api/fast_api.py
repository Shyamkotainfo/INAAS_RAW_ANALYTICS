# Backend/api/fast_api.py

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from core.query_orchestrator import QueryOrchestrator
from logger.logger import get_logger

logger = get_logger(__name__)

app = FastAPI(
    title="INAAS Analytics API",
    description="Natural language analytics over raw datasets",
    version="1.0.0"
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

orchestrator = QueryOrchestrator()


# -------------------------------
# Request Model
# -------------------------------

class QueryRequest(BaseModel):
    user_input: str


# -------------------------------
# Response Model
# -------------------------------

class QueryResponse(BaseModel):
    user_input: str
    pyspark: str | None
    results: dict | None
    insights: str | None
    error: str | None = None


# -------------------------------
# Health Check
# -------------------------------

@app.get("/health")
def health():
    return {"status": "ok"}


# -------------------------------
# Main Query Endpoint
# -------------------------------

@app.post("/query", response_model=QueryResponse)
def run_query(request: QueryRequest):
    try:
        logger.info("API Request: %s", request.user_input)

        response = orchestrator.run(request.user_input)

        return {
                "success": True,
                "data": {
                    "user_input": response.get("user_input"),
                    "pyspark": response.get("pyspark"),
                    "results": response.get("results"),
                    "insights": response.get("insights")
                },
                "error": None,
                "message": "Query processed successfully"
            }


    except Exception as e:
        logger.exception("API Error")
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )