from __future__ import annotations

import asyncio
from datetime import date

from fastapi import APIRouter, BackgroundTasks, Depends
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


@router.post("/trigger")
async def trigger_ingestion(
    body: IngestionTrigger,
    background_tasks: BackgroundTasks,
):
    """Trigger BORME ingestion for a date range."""
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
async def trigger_today(background_tasks: BackgroundTasks):
    """Trigger BORME ingestion for today."""
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
