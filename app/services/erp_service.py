"""Integración con ERPs externos (Odoo, webhook genérico).

Permite sincronizar empresas prospectadas con el ERP del usuario:
- Odoo: XML-RPC (res.partner / crm.lead)
- Webhook: POST JSON a cualquier endpoint
"""
from __future__ import annotations

import json
import logging
import xmlrpc.client
from datetime import datetime

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Company, ERPConnection, ERPSyncLog

logger = logging.getLogger(__name__)

SPAIN_COUNTRY_ID = 68  # res.country ID para España en Odoo base

DEFAULT_ODOO_MAPPING = {
    "nombre": "name",
    "cif": "vat",
    "email": "email",
    "telefono": "phone",
    "web": "website",
    "domicilio": "street",
    "localidad": "city",
}


def _build_odoo_partner(company: Company, mapping: dict | None = None) -> dict:
    m = mapping or DEFAULT_ODOO_MAPPING
    vals: dict = {"is_company": True, "company_type": "company", "country_id": SPAIN_COUNTRY_ID}
    if company.nombre:
        vals[m.get("nombre", "name")] = company.nombre
    if company.cif:
        vals[m.get("cif", "vat")] = company.cif if company.cif.startswith("ES") else f"ES{company.cif}"
    if company.email:
        vals[m.get("email", "email")] = company.email
    if company.telefono:
        vals[m.get("telefono", "phone")] = company.telefono
    if company.web:
        vals[m.get("web", "website")] = company.web
    if company.domicilio:
        vals[m.get("domicilio", "street")] = company.domicilio
    if company.localidad:
        vals[m.get("localidad", "city")] = company.localidad
    notes = []
    if company.forma_juridica:
        notes.append(f"Forma jurídica: {company.forma_juridica}")
    if company.objeto_social:
        notes.append(f"Objeto social: {company.objeto_social}")
    if company.capital_social:
        notes.append(f"Capital social: {company.capital_social:,.0f}€")
    if company.score_solvencia is not None:
        notes.append(f"Score solvencia FENIX: {company.score_solvencia}/100")
    if company.fecha_constitucion:
        notes.append(f"Constituida: {company.fecha_constitucion}")
    if notes:
        vals["comment"] = "\n".join(notes)
    return vals


class OdooClient:
    def __init__(self, url: str, database: str, username: str, api_key: str):
        self.url = url.rstrip("/")
        self.db = database
        self.user = username
        self.key = api_key
        self._uid: int | None = None

    def _common(self):
        return xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common")

    def _object(self):
        return xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object")

    def authenticate(self) -> int:
        uid = self._common().authenticate(self.db, self.user, self.key, {})
        if not uid:
            raise ConnectionError("Autenticación Odoo fallida")
        self._uid = uid
        return uid

    def execute(self, model: str, method: str, *args, **kwargs):
        if not self._uid:
            self.authenticate()
        return self._object().execute_kw(self.db, self._uid, self.key, model, method, list(args), kwargs)

    def search(self, model: str, domain: list, limit: int = 0) -> list[int]:
        kw = {"limit": limit} if limit else {}
        return self.execute(model, "search", domain, **kw)

    def create(self, model: str, vals: dict) -> int:
        return self.execute(model, "create", [vals])

    def write(self, model: str, ids: list[int], vals: dict) -> bool:
        return self.execute(model, "write", ids, vals)

    def upsert_partner(self, company: Company, mapping: dict | None = None) -> tuple[int, str]:
        vals = _build_odoo_partner(company, mapping)
        pid = None
        if company.cif:
            vat = vals.get("vat", "")
            ids = self.search("res.partner", [["vat", "=", vat]], limit=1)
            pid = ids[0] if ids else None
        if not pid and company.nombre:
            ids = self.search("res.partner", [["name", "ilike", company.nombre], ["is_company", "=", True]], limit=1)
            pid = ids[0] if ids else None
        if pid:
            self.write("res.partner", [pid], vals)
            return pid, "updated"
        return self.create("res.partner", vals), "created"


async def test_erp_connection(conn: ERPConnection) -> dict:
    if conn.provider == "odoo":
        try:
            c = OdooClient(conn.url, conn.database or "", conn.username or "", conn.api_key or "")
            ver = c._common().version()
            uid = c.authenticate()
            return {"ok": True, "message": f"Odoo {ver.get('server_version','?')} (uid={uid})"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    elif conn.provider == "webhook":
        try:
            headers = {"Content-Type": "application/json"}
            if conn.api_key:
                headers["Authorization"] = f"Bearer {conn.api_key}"
            async with httpx.AsyncClient(timeout=10.0) as cl:
                r = await cl.post(conn.url, json={"test": True, "source": "fenix-b2b"}, headers=headers)
            return {"ok": r.status_code < 400, "message": f"HTTP {r.status_code}"}
        except Exception as e:
            return {"ok": False, "message": str(e)}
    return {"ok": False, "message": f"Proveedor no soportado: {conn.provider}"}


async def push_companies_to_erp(
    connection: ERPConnection, company_ids: list[int], db: AsyncSession, user_id: int,
) -> ERPSyncLog:
    log = ERPSyncLog(
        connection_id=connection.id, user_id=user_id,
        action="push_companies", companies_sent=len(company_ids), status="running",
    )
    db.add(log)
    await db.flush()

    companies = (await db.scalars(select(Company).where(Company.id.in_(company_ids)))).all()
    mapping = None
    if connection.field_mapping:
        try:
            mapping = json.loads(connection.field_mapping)
        except json.JSONDecodeError:
            pass

    try:
        if connection.provider == "odoo":
            cr, up, fa = _push_odoo(connection, companies, mapping)
        elif connection.provider == "webhook":
            cr, up, fa = await _push_webhook(connection, companies)
        else:
            cr, up, fa = 0, 0, len(companies)

        log.companies_created, log.companies_updated, log.companies_failed = cr, up, fa
        log.status = "ok"
        log.completed_at = datetime.utcnow()
        connection.last_sync_at = datetime.utcnow()
        connection.last_sync_status = "ok"
        connection.last_sync_message = f"{cr} creadas, {up} actualizadas"
    except Exception as e:
        log.status = "error"
        log.error_message = str(e)[:500]
        log.completed_at = datetime.utcnow()
        connection.last_sync_status = "error"
        connection.last_sync_message = str(e)[:200]
        logger.error(f"[ERP] Push failed conn={connection.id}: {e}")

    await db.commit()
    return log


def _push_odoo(conn: ERPConnection, companies: list, mapping: dict | None) -> tuple[int, int, int]:
    client = OdooClient(conn.url, conn.database or "", conn.username or "", conn.api_key or "")
    client.authenticate()
    cr = up = fa = 0
    for co in companies:
        try:
            _, action = client.upsert_partner(co, mapping)
            if action == "created":
                cr += 1
            else:
                up += 1
        except Exception as e:
            fa += 1
            logger.warning(f"[ERP/Odoo] {co.nombre}: {e}")
    return cr, up, fa


async def _push_webhook(conn: ERPConnection, companies: list) -> tuple[int, int, int]:
    cr = fa = 0
    headers = {"Content-Type": "application/json"}
    if conn.api_key:
        headers["Authorization"] = f"Bearer {conn.api_key}"
    async with httpx.AsyncClient(timeout=15.0) as cl:
        for co in companies:
            payload = {
                "source": "fenix-b2b-prospector", "action": "upsert_company",
                "data": {
                    "id": co.id, "nombre": co.nombre, "cif": co.cif,
                    "forma_juridica": co.forma_juridica, "domicilio": co.domicilio,
                    "provincia": co.provincia, "localidad": co.localidad,
                    "email": co.email, "telefono": co.telefono, "web": co.web,
                    "cnae_code": co.cnae_code, "capital_social": co.capital_social,
                    "estado": co.estado, "score_solvencia": co.score_solvencia,
                    "fecha_constitucion": str(co.fecha_constitucion) if co.fecha_constitucion else None,
                },
            }
            try:
                r = await cl.post(conn.url, json=payload, headers=headers)
                cr += 1 if r.status_code < 400 else 0
                fa += 1 if r.status_code >= 400 else 0
            except Exception:
                fa += 1
    return cr, 0, fa


async def get_user_connections(user_id: int, db: AsyncSession) -> list[ERPConnection]:
    return list((await db.scalars(
        select(ERPConnection).where(ERPConnection.user_id == user_id).order_by(ERPConnection.created_at.desc())
    )).all())


async def get_sync_logs(connection_id: int, db: AsyncSession, limit: int = 20) -> list[ERPSyncLog]:
    return list((await db.scalars(
        select(ERPSyncLog).where(ERPSyncLog.connection_id == connection_id)
        .order_by(ERPSyncLog.started_at.desc()).limit(limit)
    )).all())
