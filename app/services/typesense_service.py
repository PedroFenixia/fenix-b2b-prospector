"""Servicio de busqueda con Typesense.

Gestiona la coleccion 'companies', sincronizacion de datos y busqueda.
Usa httpx (ya dependencia del proyecto) en vez del SDK oficial.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time
from typing import Any

import httpx

from app.config import settings
from app.db.models import Company

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _headers() -> dict[str, str]:
    return {"X-TYPESENSE-API-KEY": settings.typesense_api_key}


def _url(path: str) -> str:
    return f"{settings.typesense_url.rstrip('/')}{path}"


def _date_to_ts(d: date | None) -> int:
    """date → unix timestamp (0 si None)."""
    if d is None:
        return 0
    return int(datetime.combine(d, time.min).timestamp())


def _datetime_to_ts(dt: datetime | None) -> int:
    if dt is None:
        return 0
    return int(dt.timestamp())


# ---------------------------------------------------------------------------
# Collection schema
# ---------------------------------------------------------------------------

COLLECTION_SCHEMA: dict[str, Any] = {
    "name": settings.typesense_collection,
    "fields": [
        {"name": "nombre", "type": "string", "sort": True},
        {"name": "nombre_normalizado", "type": "string", "sort": True},
        {"name": "cif", "type": "string", "facet": True, "optional": True},
        {"name": "objeto_social", "type": "string", "optional": True},
        {"name": "forma_juridica", "type": "string", "facet": True, "optional": True},
        {"name": "provincia", "type": "string", "facet": True, "sort": True, "optional": True},
        {"name": "localidad", "type": "string", "facet": True, "optional": True},
        {"name": "cnae_code", "type": "string", "facet": True, "optional": True},
        {"name": "estado", "type": "string", "facet": True},
        {"name": "capital_social", "type": "float", "sort": True},
        {"name": "score_solvencia", "type": "int32", "facet": True, "sort": True},
        {"name": "fecha_ultima_publicacion", "type": "int64", "sort": True},
        {"name": "fecha_constitucion", "type": "int64", "sort": True},
        {"name": "email", "type": "string", "optional": True},
        {"name": "telefono", "type": "string", "optional": True},
        {"name": "web", "type": "string", "optional": True},
    ],
    "default_sorting_field": "fecha_ultima_publicacion",
    "token_separators": ["-", "/", "."],
}


# ---------------------------------------------------------------------------
# Company → Typesense document
# ---------------------------------------------------------------------------

def company_to_document(c: Company) -> dict[str, Any]:
    """Convierte un ORM Company a documento Typesense."""
    return {
        "id": str(c.id),
        "nombre": c.nombre or "",
        "nombre_normalizado": c.nombre_normalizado or "",
        "cif": c.cif or "",
        "objeto_social": c.objeto_social or "",
        "forma_juridica": c.forma_juridica or "",
        "provincia": c.provincia or "",
        "localidad": c.localidad or "",
        "cnae_code": c.cnae_code or "",
        "estado": c.estado or "activa",
        "capital_social": float(c.capital_social or 0),
        "score_solvencia": c.score_solvencia or 0,
        "fecha_ultima_publicacion": _date_to_ts(c.fecha_ultima_publicacion),
        "fecha_constitucion": _date_to_ts(c.fecha_constitucion),
        "email": c.email or "",
        "telefono": c.telefono or "",
        "web": c.web or "",
    }


# ---------------------------------------------------------------------------
# Collection management
# ---------------------------------------------------------------------------

async def ensure_collection() -> bool:
    """Crea la coleccion si no existe. Retorna True si se creo nueva."""
    col = settings.typesense_collection
    async with httpx.AsyncClient(timeout=10) as client:
        # Check if exists
        resp = await client.get(_url(f"/collections/{col}"), headers=_headers())
        if resp.status_code == 200:
            logger.info("Coleccion '%s' ya existe", col)
            return False

        # Create
        resp = await client.post(
            _url("/collections"),
            headers=_headers(),
            json=COLLECTION_SCHEMA,
        )
        resp.raise_for_status()
        logger.info("Coleccion '%s' creada", col)
        return True


async def drop_collection() -> None:
    """Elimina la coleccion (para recrear)."""
    col = settings.typesense_collection
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.delete(_url(f"/collections/{col}"), headers=_headers())
        if resp.status_code == 200:
            logger.info("Coleccion '%s' eliminada", col)
        elif resp.status_code == 404:
            logger.info("Coleccion '%s' no existia", col)
        else:
            resp.raise_for_status()


# ---------------------------------------------------------------------------
# Document upsert (batch)
# ---------------------------------------------------------------------------

async def upsert_documents(docs: list[dict[str, Any]], batch_size: int = 200) -> dict[str, int]:
    """Importa documentos en batch usando JSONL. Retorna {success, errors}."""
    col = settings.typesense_collection
    stats = {"success": 0, "errors": 0}

    async with httpx.AsyncClient(timeout=60) as client:
        for i in range(0, len(docs), batch_size):
            batch = docs[i : i + batch_size]
            # Typesense import expects JSONL
            jsonl = "\n".join(json.dumps(doc, ensure_ascii=False) for doc in batch)

            resp = await client.post(
                _url(f"/collections/{col}/documents/import"),
                headers={**_headers(), "Content-Type": "text/plain"},
                params={"action": "upsert"},
                content=jsonl,
            )
            resp.raise_for_status()

            # Parse line-by-line results
            for line in resp.text.strip().split("\n"):
                result = json.loads(line)
                if result.get("success"):
                    stats["success"] += 1
                else:
                    stats["errors"] += 1
                    logger.warning("Upsert error: %s", result)

    return stats


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

async def search_typesense(
    q: str = "*",
    query_by: str = "nombre,nombre_normalizado,cif,objeto_social",
    filter_by: str = "",
    sort_by: str = "fecha_ultima_publicacion:desc",
    page: int = 1,
    per_page: int = 25,
) -> dict[str, Any]:
    """Busca en Typesense. Retorna la respuesta cruda de la API.

    Claves utiles del resultado:
      - found: total de resultados
      - hits: lista de {document, highlights, text_match}
      - page: pagina actual
    """
    col = settings.typesense_collection
    params: dict[str, Any] = {
        "q": q or "*",
        "query_by": query_by,
        "sort_by": sort_by,
        "page": page,
        "per_page": per_page,
        "highlight_full_fields": "nombre,nombre_normalizado",
    }
    if filter_by:
        params["filter_by"] = filter_by

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(
            _url(f"/collections/{col}/documents/search"),
            headers=_headers(),
            params=params,
        )
        resp.raise_for_status()
        return resp.json()


# ---------------------------------------------------------------------------
# Synonyms
# ---------------------------------------------------------------------------

async def sync_synonyms() -> int:
    """Carga los grupos de sinonimos de fts_service en Typesense.

    Retorna la cantidad de grupos sincronizados.
    """
    from app.services.fts_service import SYNONYM_GROUPS

    col = settings.typesense_collection
    count = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for idx, group in enumerate(SYNONYM_GROUPS):
            synonyms = list(group)
            name = f"synonym-group-{idx}"
            body = {"synonyms": synonyms}
            resp = await client.put(
                _url(f"/collections/{col}/synonyms/{name}"),
                headers=_headers(),
                json=body,
            )
            if resp.status_code in (200, 201):
                count += 1
            else:
                logger.warning("Error syncing synonym group %d: %s", idx, resp.text)

    logger.info("Sincronizados %d grupos de sinonimos", count)
    return count
