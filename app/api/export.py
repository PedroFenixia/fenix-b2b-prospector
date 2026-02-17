from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import PLAN_LIMITS
from app.db.engine import get_db
from app.schemas.search import SearchFilters
from app.services.export_service import export_csv, export_excel

router = APIRouter()


def _check_export_limit(request: Request) -> dict | None:
    """Check if user can export. Returns error dict or None if OK."""
    user = getattr(request.state, "user", None)
    if not user:
        return {"error": "No autenticado"}
    limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
    if limits["exports"] != -1:
        # We'll check the actual count in the service; here just return user info
        pass
    return None


@router.get("/csv")
async def export_to_csv(
    request: Request,
    q: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export filtered companies as CSV."""
    user = getattr(request.state, "user", None)
    user_id = user["user_id"] if user else None

    # Check export limit
    if user:
        limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
        if limits["exports"] != -1:
            from app.db.models import User
            db_user = await db.get(User, user_id)
            if db_user and db_user.exports_this_month >= limits["exports"]:
                return JSONResponse(
                    {"error": f"Has alcanzado el limite de {limits['exports']} exportaciones/mes. Mejora tu plan."},
                    status_code=403,
                )

    filters = SearchFilters(
        q=q,
        provincia=provincia,
        forma_juridica=forma_juridica,
        cnae_code=cnae_code,
        estado=estado,
        pub_desde=pub_desde,
        pub_hasta=pub_hasta,
    )
    filepath = await export_csv(filters, db, user_id=user_id)
    return FileResponse(
        filepath,
        media_type="text/csv",
        filename=filepath.name,
        headers={"Content-Disposition": f"attachment; filename={filepath.name}"},
    )


@router.get("/excel")
async def export_to_excel(
    request: Request,
    q: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export filtered companies as Excel."""
    user = getattr(request.state, "user", None)
    user_id = user["user_id"] if user else None

    # Check export limit
    if user:
        limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
        if limits["exports"] != -1:
            from app.db.models import User
            db_user = await db.get(User, user_id)
            if db_user and db_user.exports_this_month >= limits["exports"]:
                return JSONResponse(
                    {"error": f"Has alcanzado el limite de {limits['exports']} exportaciones/mes. Mejora tu plan."},
                    status_code=403,
                )

    filters = SearchFilters(
        q=q,
        provincia=provincia,
        forma_juridica=forma_juridica,
        cnae_code=cnae_code,
        estado=estado,
        pub_desde=pub_desde,
        pub_hasta=pub_hasta,
    )
    filepath = await export_excel(filters, db, user_id=user_id)
    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filepath.name,
        headers={"Content-Disposition": f"attachment; filename={filepath.name}"},
    )
