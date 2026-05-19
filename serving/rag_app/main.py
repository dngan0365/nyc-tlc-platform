"""
serving/rag_app/app.py
FastAPI application for the NYC TLC RAG serving layer.

Endpoints
---------
POST /query          Natural-language → Athena SQL → grounded answer
GET  /health         Liveness probe
GET  /metrics        Prometheus-style counters (request count, latency p95)
GET  /examples       Returns canned example questions for the UI

Middleware
----------
- RequestID injection (X-Request-ID header)
- Structured JSON logging (per-request)
- Prometheus-compatible metrics accumulation
- CORS (configurable via env)
- Global exception handler → consistent error envelope
"""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any

import boto3
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings

from rag_chain import NycTlcRagChain, RagAnswer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

class Settings(BaseSettings):
    # AWS / Athena
    athena_database: str = "nyc_tlc_gold"
    athena_workgroup: str = "nyc-tlc-dev"
    athena_results_bucket: str = "s3://nyc-tlc-athena-results-dev/"
    aws_region: str = "us-east-1"

    # LLM
    anthropic_model: str = "claude-sonnet-4-20250514"
    max_sql_retries: int = 2

    # API
    cors_origins: list[str] = ["*"]
    log_level: str = "INFO"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=settings.log_level,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}',
)
logger = logging.getLogger("nyc_tlc.api")

# ---------------------------------------------------------------------------
# In-process metrics store (swap for prometheus_client in production)
# ---------------------------------------------------------------------------

class _Metrics:
    def __init__(self) -> None:
        self.requests_total: int = 0
        self.requests_failed: int = 0
        self.latencies_ms: list[float] = []

    def record(self, latency_ms: float, *, failed: bool = False) -> None:
        self.requests_total += 1
        self.latencies_ms.append(latency_ms)
        if failed:
            self.requests_failed += 1
        # Keep only the last 1000 samples to bound memory
        if len(self.latencies_ms) > 1000:
            self.latencies_ms = self.latencies_ms[-1000:]

    @property
    def p95_ms(self) -> float:
        if not self.latencies_ms:
            return 0.0
        sorted_latencies = sorted(self.latencies_ms)
        idx = int(len(sorted_latencies) * 0.95)
        return sorted_latencies[min(idx, len(sorted_latencies) - 1)]


_metrics = _Metrics()

# ---------------------------------------------------------------------------
# Application lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

_rag_chain: NycTlcRagChain | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialise heavy resources once at startup, tear down on shutdown."""
    global _rag_chain
    logger.info('"Initialising RAG chain"')
    _rag_chain = NycTlcRagChain(
        athena_database=settings.athena_database,
        athena_workgroup=settings.athena_workgroup,
        athena_results_location=settings.athena_results_bucket,
        aws_region=settings.aws_region,
        model_id=settings.anthropic_model,
        max_sql_retries=settings.max_sql_retries,
    )
    logger.info('"RAG chain ready"')
    yield
    logger.info('"Shutting down RAG chain"')
    _rag_chain = None


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="NYC TLC RAG API",
    description="Natural-language query interface over the NYC TLC data lakehouse (Athena/Gold layer).",
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Middleware: request ID + structured logging
# ---------------------------------------------------------------------------

@app.middleware("http")
async def request_id_middleware(request: Request, call_next) -> Response:
    request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
    request.state.request_id = request_id
    request.state.start_time = time.perf_counter()

    response = await call_next(request)

    elapsed_ms = (time.perf_counter() - request.state.start_time) * 1000
    response.headers["X-Request-ID"] = request_id
    response.headers["X-Response-Time-Ms"] = f"{elapsed_ms:.1f}"

    logger.info(
        '"path":"%s","method":"%s","status":%d,"latency_ms":%.1f,"request_id":"%s"',
        request.url.path,
        request.method,
        response.status_code,
        elapsed_ms,
        request_id,
    )
    return response


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    request_id = getattr(request.state, "request_id", "unknown")
    logger.exception('"Unhandled exception","request_id":"%s"', request_id)
    _metrics.record(0, failed=True)
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal_server_error",
            "message": str(exc),
            "request_id": request_id,
        },
    )


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    question: str = Field(
        ...,
        min_length=5,
        max_length=512,
        examples=["What was the average fare for yellow cab trips in January 2023?"],
    )
    max_rows: int = Field(default=50, ge=1, le=500)
    include_sql: bool = Field(
        default=False,
        description="Return the generated Athena SQL alongside the answer.",
    )


class QueryResponse(BaseModel):
    answer: str
    sql: str | None = None
    rows_returned: int
    execution_time_ms: float
    request_id: str


class HealthResponse(BaseModel):
    status: str
    athena_reachable: bool
    version: str = "1.0.0"


class MetricsResponse(BaseModel):
    requests_total: int
    requests_failed: int
    p95_latency_ms: float


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["ops"])
async def health(request: Request) -> HealthResponse:
    """Liveness + shallow dependency check."""
    athena_ok = False
    try:
        client = boto3.client("athena", region_name=settings.aws_region)
        client.get_work_group(WorkGroup=settings.athena_workgroup)
        athena_ok = True
    except Exception:
        pass

    return HealthResponse(
        status="ok" if athena_ok else "degraded",
        athena_reachable=athena_ok,
    )


@app.get("/metrics", response_model=MetricsResponse, tags=["ops"])
async def metrics() -> MetricsResponse:
    """Lightweight in-process metrics snapshot."""
    return MetricsResponse(
        requests_total=_metrics.requests_total,
        requests_failed=_metrics.requests_failed,
        p95_latency_ms=round(_metrics.p95_ms, 1),
    )


@app.get("/examples", tags=["query"])
async def examples() -> dict[str, list[str]]:
    """Canned example questions for UI / docs."""
    return {
        "examples": [
            "What was the average fare for yellow cab trips in January 2023?",
            "Which pickup zone had the most trips in the last 3 months?",
            "Show me hourly trip counts for Manhattan on a typical weekday.",
            "What percentage of trips used credit card payment in Q1 2023?",
            "Compare average trip distance between yellow and green cabs.",
        ]
    }


@app.post("/query", response_model=QueryResponse, tags=["query"])
async def query(body: QueryRequest, request: Request) -> QueryResponse:
    """
    Convert a natural-language question into Athena SQL, execute it,
    and return a grounded plain-English answer.
    """
    if _rag_chain is None:
        raise HTTPException(status_code=503, detail="RAG chain not initialised.")

    t0 = time.perf_counter()
    request_id = getattr(request.state, "request_id", str(uuid.uuid4()))

    try:
        result: RagAnswer = await _rag_chain.ainvoke(
            question=body.question,
            max_rows=body.max_rows,
        )
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        _metrics.record(elapsed_ms, failed=True)
        logger.error(
            '"Query failed","question":"%s","error":"%s","request_id":"%s"',
            body.question,
            str(exc),
            request_id,
        )
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    elapsed_ms = (time.perf_counter() - t0) * 1000
    _metrics.record(elapsed_ms)

    return QueryResponse(
        answer=result.answer,
        sql=result.sql if body.include_sql else None,
        rows_returned=result.rows_returned,
        execution_time_ms=round(elapsed_ms, 1),
        request_id=request_id,
    )