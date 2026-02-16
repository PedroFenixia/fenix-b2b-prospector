"""API endpoints para vigilancia y alertas."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.services.watchlist_service import (
    add_to_watchlist,
    mark_alert_read,
    mark_all_read,
    remove_from_watchlist,
)

router = APIRouter()


class WatchlistBody(BaseModel):
    notas: str | None = None


@router.post("/{company_id}")
async def api_add_to_watchlist(
    company_id: int,
    body: WatchlistBody = WatchlistBody(),
    db: AsyncSession = Depends(get_db),
):
    entry = await add_to_watchlist(company_id, body.notas, db)
    return {"ok": True, "id": entry.id}


@router.delete("/{company_id}")
async def api_remove_from_watchlist(
    company_id: int,
    db: AsyncSession = Depends(get_db),
):
    removed = await remove_from_watchlist(company_id, db)
    return {"ok": removed}


@router.post("/alerts/{alert_id}/read")
async def api_mark_alert_read(
    alert_id: int,
    db: AsyncSession = Depends(get_db),
):
    ok = await mark_alert_read(alert_id, db)
    return {"ok": ok}


@router.post("/alerts/read-all")
async def api_mark_all_read(db: AsyncSession = Depends(get_db)):
    count = await mark_all_read(db)
    return {"ok": True, "count": count}
