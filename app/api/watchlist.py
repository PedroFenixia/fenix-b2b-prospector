"""API endpoints para vigilancia y alertas."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
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


def _user_id(request: Request) -> int | None:
    user = getattr(request.state, "user", None)
    return user["user_id"] if user else None


class WatchlistBody(BaseModel):
    notas: str | None = None
    tipos_acto: list[str] | None = None


@router.post("/{company_id}")
async def api_add_to_watchlist(
    company_id: int,
    request: Request,
    body: WatchlistBody = WatchlistBody(),
    db: AsyncSession = Depends(get_db),
):
    uid = _user_id(request)
    entry = await add_to_watchlist(company_id, body.notas, db, body.tipos_acto, user_id=uid)
    return {"ok": True, "id": entry.id}


@router.delete("/{company_id}")
async def api_remove_from_watchlist(
    company_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = _user_id(request)
    removed = await remove_from_watchlist(company_id, db, user_id=uid)
    return {"ok": removed}


@router.post("/alerts/{alert_id}/read")
async def api_mark_alert_read(
    alert_id: int,
    db: AsyncSession = Depends(get_db),
):
    ok = await mark_alert_read(alert_id, db)
    return {"ok": ok}


@router.post("/alerts/read-all")
async def api_mark_all_read(request: Request, db: AsyncSession = Depends(get_db)):
    uid = _user_id(request)
    count = await mark_all_read(db, user_id=uid)
    return {"ok": True, "count": count}
