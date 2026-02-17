"""API endpoints para integración ERP."""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_current_user
from app.db.engine import get_db
from app.db.models import ERPConnection, ERPSyncLog
from app.services.erp_service import (
    get_sync_logs,
    get_user_connections,
    push_companies_to_erp,
    test_erp_connection,
)

router = APIRouter()


# --- Schemas ---

class ERPConnectionCreate(BaseModel):
    provider: str  # odoo | webhook
    name: str = "Mi ERP"
    url: str
    database: str | None = None
    username: str | None = None
    api_key: str | None = None
    field_mapping: str | None = None  # JSON string


class ERPConnectionOut(BaseModel):
    id: int
    provider: str
    name: str
    url: str
    database: str | None
    is_active: bool
    last_sync_at: str | None
    last_sync_status: str | None
    last_sync_message: str | None

    class Config:
        from_attributes = True


class ERPPushRequest(BaseModel):
    connection_id: int
    company_ids: list[int]


class ERPSyncLogOut(BaseModel):
    id: int
    action: str
    companies_sent: int
    companies_created: int
    companies_updated: int
    companies_failed: int
    status: str
    error_message: str | None
    started_at: str
    completed_at: str | None

    class Config:
        from_attributes = True


# --- Helpers ---

def _require_auth(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(401, "No autenticado")
    return user


async def _get_conn(conn_id: int, user_id: int, db: AsyncSession) -> ERPConnection:
    conn = await db.get(ERPConnection, conn_id)
    if not conn or conn.user_id != user_id:
        raise HTTPException(404, "Conexión ERP no encontrada")
    return conn


# --- Endpoints ---

@router.get("/connections")
async def list_connections(request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    conns = await get_user_connections(user["user_id"], db)
    return [
        {
            "id": c.id, "provider": c.provider, "name": c.name, "url": c.url,
            "database": c.database, "is_active": c.is_active,
            "last_sync_at": str(c.last_sync_at) if c.last_sync_at else None,
            "last_sync_status": c.last_sync_status,
            "last_sync_message": c.last_sync_message,
        }
        for c in conns
    ]


@router.post("/connections")
async def create_connection(body: ERPConnectionCreate, request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    if body.provider not in ("odoo", "webhook"):
        raise HTTPException(400, "Proveedor debe ser 'odoo' o 'webhook'")
    conn = ERPConnection(
        user_id=user["user_id"],
        provider=body.provider,
        name=body.name,
        url=body.url,
        database=body.database,
        username=body.username,
        api_key=body.api_key,
        field_mapping=body.field_mapping,
    )
    db.add(conn)
    await db.commit()
    await db.refresh(conn)
    return {"id": conn.id, "message": "Conexión ERP creada"}


@router.delete("/connections/{conn_id}")
async def delete_connection(conn_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    conn = await _get_conn(conn_id, user["user_id"], db)
    await db.delete(conn)
    await db.commit()
    return {"message": "Conexión eliminada"}


@router.post("/connections/{conn_id}/test")
async def test_connection(conn_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    conn = await _get_conn(conn_id, user["user_id"], db)
    result = await test_erp_connection(conn)
    return result


@router.post("/push")
async def push_to_erp(body: ERPPushRequest, request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    conn = await _get_conn(body.connection_id, user["user_id"], db)
    if not conn.is_active:
        raise HTTPException(400, "Conexión ERP desactivada")
    if not body.company_ids:
        raise HTTPException(400, "Debes indicar al menos una empresa")
    if len(body.company_ids) > 500:
        raise HTTPException(400, "Máximo 500 empresas por push")

    log = await push_companies_to_erp(conn, body.company_ids, db, user["user_id"])
    return {
        "status": log.status,
        "companies_created": log.companies_created,
        "companies_updated": log.companies_updated,
        "companies_failed": log.companies_failed,
        "error_message": log.error_message,
    }


@router.get("/connections/{conn_id}/logs")
async def list_sync_logs(conn_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    user = _require_auth(request)
    await _get_conn(conn_id, user["user_id"], db)  # verify ownership
    logs = await get_sync_logs(conn_id, db)
    return [
        {
            "id": l.id, "action": l.action, "companies_sent": l.companies_sent,
            "companies_created": l.companies_created, "companies_updated": l.companies_updated,
            "companies_failed": l.companies_failed, "status": l.status,
            "error_message": l.error_message,
            "started_at": str(l.started_at), "completed_at": str(l.completed_at) if l.completed_at else None,
        }
        for l in logs
    ]
