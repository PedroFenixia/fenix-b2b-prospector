"""Admin API endpoints for user management."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db

admin_router = APIRouter(prefix="/api/admin", tags=["admin"])


def _require_admin(request: Request):
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "admin":
        return None
    return user


@admin_router.patch("/users/{user_id}")
async def update_user(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    if not _require_admin(request):
        return JSONResponse({"error": "No autorizado"}, status_code=403)

    from app.db.models import User
    user = await db.get(User, user_id)
    if not user:
        return JSONResponse({"error": "Usuario no encontrado"}, status_code=404)

    body = await request.json()

    allowed_fields = {"role", "plan", "is_active"}
    for field, value in body.items():
        if field in allowed_fields:
            setattr(user, field, value)

    await db.commit()
    return {"ok": True, "user_id": user_id}


@admin_router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    request: Request,
    user_id: int,
    db: AsyncSession = Depends(get_db),
):
    if not _require_admin(request):
        return JSONResponse({"error": "No autorizado"}, status_code=403)

    from app.db.models import User
    from app.auth import hash_password_async

    user = await db.get(User, user_id)
    if not user:
        return JSONResponse({"error": "Usuario no encontrado"}, status_code=404)

    body = await request.json()
    password = body.get("password", "")
    if len(password) < 6:
        return JSONResponse({"error": "Minimo 6 caracteres"}, status_code=400)

    user.password_hash = await hash_password_async(password)
    await db.commit()
    return {"ok": True, "user_id": user_id}
