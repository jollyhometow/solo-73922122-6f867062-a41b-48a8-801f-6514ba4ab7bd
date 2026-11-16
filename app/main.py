"""FastAPI application: meter ownership registration, ingestion, correction
and downstream queries. Run two instances against one PostgreSQL database
(see docker-compose.yml) to exercise the concurrency guarantees.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Query, Request
from fastapi.responses import JSONResponse

from . import services
from .config import settings
from .db import close_pool, init_pool
from .errors import AppError
from .schemas import (
    CorrectRequest,
    IngestRequest,
    IngestResponse,
    MeterOut,
    OwnershipOut,
    ReadingOut,
    RegisterRequest,
    VersionedResponse,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_pool()
    try:
        yield
    finally:
        close_pool()


app = FastAPI(
    title="Meter Ownership Warehouse API",
    version="1.0.0",
    lifespan=lifespan,
    description=(
        "Register non-overlapping meter-to-line attribution segments, ingest "
        "readings bound to the segment covering their UTC timestamp, correct "
        "a segment window with optimistic version control (re-linking "
        "warehoused readings), and query readings by production line."
    ),
)


@app.exception_handler(AppError)
async def _app_error_handler(request: Request, exc: AppError) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})


def conn():
    """Yield a pooled connection; the service layer owns the transaction."""
    with init_pool().connection() as c:
        yield c


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok", "time": datetime.now(timezone.utc)}


@app.post("/ownership/register", response_model=VersionedResponse,
          tags=["ownership"], status_code=201,
          summary="Register a non-overlapping attribution segment")
def register(body: RegisterRequest, c=Depends(conn)) -> VersionedResponse:
    version = services.register_ownership(
        c, body.meter_no, body.valid_from, body.valid_to, body.line
    )
    return VersionedResponse(meter_no=body.meter_no, version=version)


@app.get("/ownership/{meter_no}", response_model=list[OwnershipOut],
         tags=["ownership"], summary="List current attribution segments")
def list_ownership(meter_no: str, c=Depends(conn)) -> list[OwnershipOut]:
    return [OwnershipOut(**row) for row in services.list_ownership(c, meter_no)]


@app.get("/meters/{meter_no}", response_model=MeterOut, tags=["ownership"],
         summary="Get a meter and its current version")
def get_meter(meter_no: str, c=Depends(conn)) -> MeterOut:
    return MeterOut(**services.get_meter(c, meter_no))


@app.post("/readings/ingest", response_model=IngestResponse,
          tags=["readings"], status_code=201,
          summary="Ingest readings; reject any without covering ownership")
def ingest(body: IngestRequest, c=Depends(conn)) -> IngestResponse:
    rows = services.ingest_readings(c, [r.model_dump() for r in body.readings])
    return IngestResponse(
        ingested=len(rows), readings=[ReadingOut(**r) for r in rows]
    )


@app.post("/ownership/correct", response_model=VersionedResponse,
          tags=["ownership"],
          summary="Replace the line on one [from,to) window; keep both sides")
def correct(body: CorrectRequest, c=Depends(conn)) -> VersionedResponse:
    new_version = services.correct_ownership(
        c, body.meter_no, body.valid_from, body.valid_to,
        body.new_line, body.base_version,
    )
    return VersionedResponse(meter_no=body.meter_no, version=new_version)


@app.get("/readings/by-line/{line}", response_model=list[ReadingOut],
         tags=["readings"],
         summary="Committed readings currently attributed to a line")
def by_line(
    line: str,
    c=Depends(conn),
    start: Optional[datetime] = Query(None, description="UTC, inclusive"),
    end: Optional[datetime] = Query(None, description="UTC, exclusive"),
    limit: int = Query(200, ge=1),
) -> list[ReadingOut]:
    limit = min(limit, settings.query_limit_max)
    rows = services.query_readings_by_line(c, line, start, end, limit)
    return [ReadingOut(**r) for r in rows]
