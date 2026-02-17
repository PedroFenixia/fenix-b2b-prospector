from __future__ import annotations

import asyncio
from datetime import date

from fastapi import APIRouter, BackgroundTasks, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.db.models import IngestionLog
from app.schemas.ingestion import IngestionLogOut, IngestionStatus, IngestionTrigger
from app.services.ingestion_orchestrator import (
    get_ingestion_status,
    ingest_date_range,
    ingest_single_date,
)

router = APIRouter()


def _require_admin(request: Request):
    """Return error response if user is not admin, None if OK."""
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "admin":
        return JSONResponse({"error": "Solo administradores"}, status_code=403)
    return None


@router.post("/trigger")
async def trigger_ingestion(
    request: Request,
    body: IngestionTrigger,
    background_tasks: BackgroundTasks,
):
    """Trigger BORME ingestion for a date range."""
    err = _require_admin(request)
    if err:
        return err
    status = get_ingestion_status()
    if status.get("is_running"):
        return {"error": "Ingestion already running", "status": status}

    background_tasks.add_task(ingest_date_range, body.fecha_desde, body.fecha_hasta)
    days = (body.fecha_hasta - body.fecha_desde).days + 1
    return {
        "message": f"Ingestion started for {days} days",
        "fecha_desde": str(body.fecha_desde),
        "fecha_hasta": str(body.fecha_hasta),
    }


@router.post("/trigger-today")
async def trigger_today(request: Request, background_tasks: BackgroundTasks):
    """Trigger BORME ingestion for today."""
    err = _require_admin(request)
    if err:
        return err
    today = date.today()
    status = get_ingestion_status()
    if status.get("is_running"):
        return {"error": "Ingestion already running", "status": status}

    background_tasks.add_task(ingest_single_date, today)
    return {"message": f"Ingestion started for {today}"}


@router.get("/status")
async def ingestion_status(db: AsyncSession = Depends(get_db)):
    """Get current ingestion status and recent jobs."""
    status = get_ingestion_status()
    recent = await db.scalars(
        select(IngestionLog)
        .order_by(IngestionLog.fecha_borme.desc())
        .limit(20)
    )
    return IngestionStatus(
        is_running=status.get("is_running", False),
        current_date=status.get("current_date"),
        recent_jobs=[IngestionLogOut.model_validate(j) for j in recent.all()],
    )


@router.get("/log", response_model=list[IngestionLogOut])
async def ingestion_log(
    page: int = 1,
    per_page: int = 50,
    db: AsyncSession = Depends(get_db),
):
    result = await db.scalars(
        select(IngestionLog)
        .order_by(IngestionLog.fecha_borme.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    return [IngestionLogOut.model_validate(j) for j in result.all()]


@router.post("/enrich-cif")
async def enrich_cif(
    request: Request,
    background_tasks: BackgroundTasks,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """Trigger CIF enrichment for companies missing CIF."""
    err = _require_admin(request)
    if err:
        return err
    from app.config import settings
    if not settings.apiempresas_key:
        return {"error": "APIEMPRESAS_KEY not configured in .env"}

    from app.services.cif_enrichment import count_missing_cif, enrich_batch

    stats = await count_missing_cif(db)

    async def _run_enrichment():
        from app.db.engine import async_session
        async with async_session() as session:
            result = await enrich_batch(session, settings.apiempresas_key, limit=limit)
            return result

    background_tasks.add_task(_run_enrichment)
    return {
        "message": f"CIF enrichment started (batch of {limit})",
        "missing_cif": stats,
    }


@router.get("/cif-stats")
async def cif_stats(db: AsyncSession = Depends(get_db)):
    """Get CIF coverage statistics."""
    from app.services.cif_enrichment import count_missing_cif
    return await count_missing_cif(db)


@router.post("/enrich-web")
async def enrich_web(
    request: Request,
    background_tasks: BackgroundTasks,
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    """Trigger web enrichment: search company websites for CIF, email, phone."""
    err = _require_admin(request)
    if err:
        return err
    from app.services.web_enrichment import count_web_coverage, enrich_batch_web

    stats = await count_web_coverage(db)

    async def _run_web_enrichment():
        from app.db.engine import async_session
        async with async_session() as session:
            await enrich_batch_web(session, limit=limit)

    background_tasks.add_task(_run_web_enrichment)
    return {
        "message": f"Web enrichment started (batch of {limit})",
        "coverage": stats,
    }


@router.get("/web-stats")
async def web_stats(db: AsyncSession = Depends(get_db)):
    """Get web enrichment coverage statistics."""
    from app.services.web_enrichment import count_web_coverage
    return await count_web_coverage(db)


@router.post("/score-batch")
async def score_batch_endpoint(
    request: Request,
    background_tasks: BackgroundTasks,
    limit: int = 500,
):
    """Score a batch of companies for solvency."""
    err = _require_admin(request)
    if err:
        return err
    from app.services.scoring_service import score_batch

    async def _run_scoring():
        from app.db.engine import async_session
        async with async_session() as session:
            result = await score_batch(session, limit=limit)
            return result

    background_tasks.add_task(_run_scoring)
    return {"message": f"Scoring started (batch of {limit})"}


@router.get("/score-stats")
async def score_stats(db: AsyncSession = Depends(get_db)):
    """Get solvency score coverage statistics."""
    from app.services.scoring_service import get_score_stats
    return await get_score_stats(db)
