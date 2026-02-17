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
    db: AsyncSession = Depends(get_db),
):
    """Trigger full CIF + web enrichment for ALL companies."""
    err = _require_admin(request)
    if err:
        return err
    from app.scheduler import full_enrichment, is_enrichment_running
    from app.services.cif_enrichment import count_missing_cif

    if is_enrichment_running():
        return {"error": "El enriquecimiento ya está en ejecución"}

    stats = await count_missing_cif(db)
    background_tasks.add_task(full_enrichment)
    return {
        "message": f"Enriquecimiento completo iniciado ({stats['without_cif']} sin CIF)",
        "missing_cif": stats,
    }


@router.get("/cif-stats")
async def cif_stats(db: AsyncSession = Depends(get_db)):
    """Get CIF coverage statistics."""
    from app.services.cif_enrichment import count_missing_cif
    return await count_missing_cif(db)


@router.get("/enrichment-status")
async def enrichment_status():
    """Check if enrichment is running."""
    from app.scheduler import is_enrichment_running
    return {"running": is_enrichment_running()}


@router.post("/stop-enrichment")
async def stop_enrichment_endpoint(request: Request):
    """Stop the running enrichment process."""
    err = _require_admin(request)
    if err:
        return err
    from app.scheduler import is_enrichment_running, stop_enrichment
    if not is_enrichment_running():
        return {"error": "No hay enriquecimiento en ejecución"}
    stop_enrichment()
    return {"message": "Señal de parada enviada"}


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
