"""Enriquecimiento de CIF usando APIEmpresas.es (plan Sandbox gratuito, 200 consultas/mes)."""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

import httpx
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import Company

logger = logging.getLogger(__name__)

API_BASE = "https://apiempresas.es/api/v1"
RATE_LIMIT_DELAY = 1.5  # seconds between requests to stay within limits


async def lookup_cif_by_name(nombre: str, api_key: str) -> Optional[dict]:
    """Search for a company CIF by name using APIEmpresas.es."""
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            resp = await client.get(
                f"{API_BASE}/companies/search",
                params={"name": nombre},
                headers={"X-API-KEY": api_key},
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    return data[0]  # Best match
                elif isinstance(data, dict) and data.get("data"):
                    results = data["data"]
                    if results:
                        return results[0]
            elif resp.status_code == 429:
                logger.warning("APIEmpresas rate limit reached")
                return None
            elif resp.status_code == 401:
                logger.error("APIEmpresas API key invalid")
                return None
            else:
                logger.warning(f"APIEmpresas returned {resp.status_code} for '{nombre}'")
        except Exception as e:
            logger.error(f"APIEmpresas error for '{nombre}': {e}")
    return None


async def enrich_company_cif(company_id: int, db: AsyncSession, api_key: str) -> Optional[str]:
    """Lookup and store CIF for a single company."""
    company = await db.get(Company, company_id)
    if not company or company.cif:
        return company.cif if company else None

    result = await lookup_cif_by_name(company.nombre, api_key)
    if result:
        cif = result.get("cif") or result.get("nif")
        if cif:
            company.cif = cif
            await db.commit()
            logger.info(f"CIF enriched: {company.nombre} -> {cif}")
            return cif
    return None


async def enrich_batch(
    db: AsyncSession,
    api_key: str,
    limit: int = 50,
) -> dict:
    """Enrich a batch of companies that don't have CIF.

    Returns stats: total attempted, found, not found.
    """
    companies = await db.scalars(
        select(Company)
        .where(Company.cif.is_(None))
        .order_by(Company.fecha_ultima_publicacion.desc())
        .limit(limit)
    )

    stats = {"attempted": 0, "found": 0, "not_found": 0, "errors": 0}

    for company in companies.all():
        stats["attempted"] += 1
        try:
            result = await lookup_cif_by_name(company.nombre, api_key)
            if result:
                cif = result.get("cif") or result.get("nif")
                if cif:
                    company.cif = cif
                    stats["found"] += 1
                    logger.info(f"CIF: {company.nombre} -> {cif}")
                else:
                    stats["not_found"] += 1
            else:
                stats["not_found"] += 1
        except Exception as e:
            logger.error(f"CIF enrichment error for {company.nombre}: {e}")
            stats["errors"] += 1

        await asyncio.sleep(RATE_LIMIT_DELAY)

    await db.commit()
    return stats


async def count_missing_cif(db: AsyncSession) -> dict:
    """Get stats on CIF coverage."""
    total = await db.scalar(select(func.count(Company.id)))
    with_cif = await db.scalar(select(func.count(Company.id)).where(Company.cif.isnot(None)))
    without_cif = (total or 0) - (with_cif or 0)
    return {
        "total": total or 0,
        "with_cif": with_cif or 0,
        "without_cif": without_cif,
        "coverage_pct": round((with_cif or 0) / total * 100, 1) if total else 0,
    }
