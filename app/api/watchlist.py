"""API endpoints para vigilancia y alertas."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.services.watchlist_service import (
    add_act_type_watch,
    add_to_watchlist,
    mark_alert_read,
    mark_all_read,
    remove_act_type_watch,
    remove_from_watchlist,
)

router = APIRouter()


def _user_id(request: Request) -> int | None:
    user = getattr(request.state, "user", None)
    return user["user_id"] if user else None


# ── Fixed-path routes FIRST (before /{company_id} to avoid route conflicts) ──


class ActTypeWatchBody(BaseModel):
    tipo_acto: str
    filtro_provincia: str | None = None


@router.post("/act-types")
async def api_add_act_type_watch(
    body: ActTypeWatchBody,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = _user_id(request)
    if not uid:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Login requerido"}, status_code=401)

    # Check alert limits (admins bypass)
    user = getattr(request.state, "user", None)
    if user and user.get("role") != "admin":
        from app.auth import PLAN_LIMITS
        from sqlalchemy import func, select
        from app.db.models import ActTypeWatch
        limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
        if limits["alerts"] != -1:
            count = await db.scalar(
                select(func.count(ActTypeWatch.id)).where(
                    ActTypeWatch.user_id == uid, ActTypeWatch.is_active == True
                )
            ) or 0
            if count >= limits["alerts"]:
                from fastapi.responses import JSONResponse
                return JSONResponse(
                    {"error": f"Limite de {limits['alerts']} alertas alcanzado. Mejora tu plan."},
                    status_code=403,
                )

    try:
        entry = await add_act_type_watch(uid, body.tipo_acto, db, body.filtro_provincia)
        return {"ok": True, "id": entry.id}
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Error creating act type watch: {e}")
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Error al crear suscripcion. Intentalo de nuevo."}, status_code=500)


@router.delete("/act-types/{watch_id}")
async def api_remove_act_type_watch(
    watch_id: int,
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    uid = _user_id(request)
    if not uid:
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Login requerido"}, status_code=401)
    removed = await remove_act_type_watch(watch_id, uid, db)
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


# ── Parameterized routes LAST ──


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
    # Check watchlist limits (admins bypass)
    user = getattr(request.state, "user", None)
    if user and user.get("role") != "admin":
        from app.auth import PLAN_LIMITS
        from sqlalchemy import func, select
        from app.db.models import Watchlist
        limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
        if limits["watchlist"] != -1:
            count = await db.scalar(
                select(func.count(Watchlist.id)).where(Watchlist.user_id == uid)
            ) or 0
            if count >= limits["watchlist"]:
                from fastapi.responses import JSONResponse
                return JSONResponse(
                    {"error": f"Limite de {limits['watchlist']} empresas en vigilancia alcanzado. Mejora tu plan."},
                    status_code=403,
                )
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
