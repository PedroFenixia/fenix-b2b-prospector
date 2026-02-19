"""Enriquecimiento de CIF buscando en internet por nombre de empresa."""
from __future__ import annotations

import asyncio
import logging
import random
import re
import shlex
from collections import Counter
from typing import Optional
from urllib.parse import urlencode

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
]


def _random_ua() -> str:
    return random.choice(_USER_AGENTS)


def _clean_name(nombre: str) -> str:
    """Remove legal form suffixes for better search results."""
    cleaned = nombre.strip()
    for suffix in ["SL", "SLL", "SA", "SLU", "SAU", "SLNE", "SC", "SLP", "COOP"]:
        cleaned = re.sub(rf"\b{suffix}\b\.?$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = cleaned.rstrip(".,- ")
    return cleaned


async def _curl_fetch(url: str, timeout: int = 8) -> Optional[str]:
    """Fetch URL using curl subprocess (bypasses TLS fingerprinting)."""
    try:
        cmd = [
            "curl", "-s", "-L",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {_random_ua()}",
            "-H", "Accept-Language: es-ES,es;q=0.9",
            url,
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout + 5)
        if proc.returncode == 0 and stdout:
            return stdout.decode("utf-8", errors="replace")
    except Exception as e:
        logger.debug(f"curl error for {url}: {e}")
    return None


async def _search_ddg(nombre: str) -> Optional[str]:
    """Search DuckDuckGo HTML for CIF of a company using curl."""
    search_name = _clean_name(nombre)
    query = f'"{search_name}" CIF empresa España'
    url = f"https://html.duckduckgo.com/html/?{urlencode({'q': query})}"
    try:
        html = await _curl_fetch(url, timeout=10)
        if not html:
            return None
        cifs = CIF_RE.findall(html)
        if cifs:
            counter = Counter(cifs)
            return counter.most_common(1)[0][0]
    except Exception as e:
        logger.debug(f"DDG error for '{nombre}': {e}")
    return None


async def lookup_cif_by_name(nombre: str, **kwargs) -> Optional[str]:
    """Search free web sources for a company's CIF.

    Uses Brave Search via curl subprocess.
    """
    cif = await _search_ddg(nombre)
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
