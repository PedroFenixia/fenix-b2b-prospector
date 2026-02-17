"""Enriquecimiento de CIF buscando en internet por nombre de empresa."""
from __future__ import annotations

import asyncio
import logging
import random
import re
from collections import Counter
from typing import Optional

import httpx
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from unidecode import unidecode

from app.db.models import Company

logger = logging.getLogger(__name__)

RATE_LIMIT_DELAY = 2.0  # seconds between requests

# CIF regex: letra + 7 digitos + control (digito o letra)
CIF_RE = re.compile(r"\b([ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J])\b")

# Rotación de User-Agents para evitar detección
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


UA = _USER_AGENTS[0]  # Default for backwards compat


def _name_to_slug(nombre: str) -> str:
    """Convert company name to URL slug for empresia.es."""
    slug = unidecode(nombre).lower().strip()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug).strip("-")
    return slug


def _clean_name(nombre: str) -> str:
    """Remove legal form suffixes for better search results."""
    cleaned = nombre.strip()
    for suffix in ["SL", "SLL", "SA", "SLU", "SAU", "SLNE", "SC", "SLP", "COOP"]:
        cleaned = re.sub(rf"\b{suffix}\b\.?$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = cleaned.rstrip(".,- ")
    return cleaned


async def _search_empresia(nombre: str, client: httpx.AsyncClient) -> Optional[str]:
    """Search empresia.es by direct URL slug."""
    slug = _name_to_slug(nombre)
    url = f"https://www.empresia.es/empresa/{slug}/"
    try:
        resp = await client.get(url, headers={"User-Agent": _random_ua()})
        if resp.status_code == 429:
            logger.warning("empresia.es rate limit, backing off")
            await asyncio.sleep(30)
            return None
        if resp.status_code != 200:
            return None
        cifs = CIF_RE.findall(resp.text)
        if cifs:
            return cifs[0]
    except Exception as e:
        logger.debug(f"empresia.es error for '{nombre}': {e}")
    return None


async def _search_infocif(nombre: str, client: httpx.AsyncClient) -> Optional[str]:
    """Search infocif.es by company name."""
    search_name = _clean_name(nombre)
    url = "https://www.infocif.es/general/empresas-702702.asp"
    try:
        resp = await client.get(
            url,
            params={"Ession": search_name},
            headers={"User-Agent": _random_ua()},
        )
        if resp.status_code == 429:
            logger.warning("infocif.es rate limit, backing off")
            await asyncio.sleep(30)
            return None
        if resp.status_code != 200:
            return None
        cifs = CIF_RE.findall(resp.text)
        if cifs:
            return cifs[0]
    except Exception as e:
        logger.debug(f"infocif.es error for '{nombre}': {e}")
    return None


async def _search_google(nombre: str, client: httpx.AsyncClient) -> Optional[str]:
    """Search Google HTML for CIF of a company."""
    search_name = _clean_name(nombre)
    query = f'"{search_name}" CIF empresa España'
    try:
        resp = await client.get(
            "https://www.google.com/search",
            params={"q": query, "hl": "es", "num": "10"},
            headers={"User-Agent": UA, "Accept-Language": "es-ES,es;q=0.9"},
        )
        if resp.status_code != 200:
            return None
        cifs = CIF_RE.findall(resp.text)
        if cifs:
            counter = Counter(cifs)
            return counter.most_common(1)[0][0]
    except Exception as e:
        logger.debug(f"Google error for '{nombre}': {e}")
    return None


async def _search_duckduckgo(nombre: str, client: httpx.AsyncClient) -> Optional[str]:
    """Search DuckDuckGo HTML for CIF of a company (fallback)."""
    search_name = _clean_name(nombre)
    query = f'"{search_name}" CIF empresa'
    try:
        resp = await client.get(
            "https://html.duckduckgo.com/html/",
            params={"q": query},
            headers={"User-Agent": _random_ua()},
        )
        if resp.status_code == 429 or resp.status_code == 202:
            logger.warning("DuckDuckGo rate limit, backing off")
            await asyncio.sleep(60)
            return None
        if resp.status_code != 200:
            return None
        cifs = CIF_RE.findall(resp.text)
        if cifs:
            counter = Counter(cifs)
            return counter.most_common(1)[0][0]
    except Exception as e:
        logger.debug(f"DuckDuckGo error for '{nombre}': {e}")
    return None


_proxy_list: list[str] = []
_proxy_index = 0


def _get_proxy() -> Optional[str]:
    """Get next proxy URL from rotating list."""
    global _proxy_list, _proxy_index
    from app.config import settings
    if not _proxy_list and settings.enrichment_proxies:
        _proxy_list = [p.strip() for p in settings.enrichment_proxies.split(",") if p.strip()]
    if not _proxy_list:
        return None
    proxy = _proxy_list[_proxy_index % len(_proxy_list)]
    _proxy_index += 1
    return proxy


async def lookup_cif_by_name(nombre: str, use_google: bool = True) -> Optional[str]:
    """Search multiple free web sources for a company's CIF.

    Individual mode (use_google=True): Google -> empresia -> infocif -> DuckDuckGo
    Batch mode (use_google=False): empresia -> infocif -> DuckDuckGo (evita baneo Google)
    """
    proxy = _get_proxy()
    async with httpx.AsyncClient(timeout=12.0, follow_redirects=True, proxy=proxy) as client:
        if use_google:
            cif = await _search_google(nombre, client)
            if cif:
                return cif

        cif = await _search_empresia(nombre, client)
        if cif:
            return cif

        cif = await _search_infocif(nombre, client)
        if cif:
            return cif

        cif = await _search_duckduckgo(nombre, client)
        if cif:
            return cif

    return None


async def enrich_company_cif(company_id: int, db: AsyncSession) -> Optional[str]:
    """Lookup and store CIF for a single company via web search."""
    company = await db.get(Company, company_id)
    if not company or company.cif:
        return company.cif if company else None

    cif = await lookup_cif_by_name(company.nombre)
    if cif:
        company.cif = cif
        await db.commit()
        logger.info(f"CIF found: {company.nombre} -> {cif}")
        return cif

    return None


async def enrich_batch(db: AsyncSession, limit: int = 50) -> dict:
    """Enrich a batch of companies that don't have CIF."""
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
            cif = await lookup_cif_by_name(company.nombre)
            if cif:
                company.cif = cif
                stats["found"] += 1
                logger.info(f"CIF: {company.nombre} -> {cif}")
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
