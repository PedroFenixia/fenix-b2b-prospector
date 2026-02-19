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


def _normalize(text: str) -> str:
    """Lowercase + strip accents for fuzzy comparison."""
    return unidecode(text).lower().strip()


def _name_matches(nombre: str, page_text: str) -> bool:
    """Check if enough tokens from the company name appear in the page."""
    name_norm = _normalize(_clean_name(nombre))
    page_norm = _normalize(page_text[:5000])
    tokens = [t for t in name_norm.split() if len(t) > 2]
    if not tokens:
        return False
    found = sum(1 for t in tokens if t in page_norm)
    return found >= len(tokens) * 0.6


# CNAE regex: "CNAE" + optional "2009:" prefix + 4-digit code
_CNAE_RE = re.compile(r"CNAE[\s:\-]*(?:20\d{2}[\s:\-]+)(\d{4})")
# Address patterns
_ADDR_RE = re.compile(
    r"(?:Domicilio|Dirección|Domicilio Social|Dirección Social|Calle|C/|Avda\.|Avenida|Plaza)"
    r"[\s:]*([^<\n]{10,120})",
    re.IGNORECASE,
)
# Objeto social
_OBJ_RE = re.compile(
    r"(?:Objeto [Ss]ocial|Actividad)[\s:]*([^<\n]{15,300})",
    re.IGNORECASE,
)


async def _search_ddg(nombre: str) -> Optional[str]:
    """Search DuckDuckGo HTML for CIF of a company using curl.

    Tries two queries: exact name match, then cleaned name with CIF keyword.
    """
    queries = [
        f'"{_clean_name_full(nombre)}" CIF',
        f'{_clean_name_full(nombre)} CIF NIF empresa',
    ]
    for query in queries:
        url = f"https://html.duckduckgo.com/html/?{urlencode({'q': query})}"
        try:
            html = await _curl_fetch(url, timeout=10)
            if not html:
                continue
            cifs = CIF_RE.findall(html)
            if cifs:
                counter = Counter(cifs)
                return counter.most_common(1)[0][0]
        except Exception as e:
            logger.debug(f"DDG error for '{nombre}': {e}")
        await asyncio.sleep(1)
    return None


def _clean_name_full(nombre: str) -> str:
    """Remove ALL legal form variants from company name."""
    cleaned = nombre.strip()
    # Remove long-form suffixes first, then abbreviations
    for suffix in [
        "SOCIEDAD LIMITADA UNIPERSONAL", "SOCIEDAD LIMITADA LABORAL",
        "SOCIEDAD LIMITADA PROFESIONAL", "SOCIEDAD LIMITADA NUEVA EMPRESA",
        "SOCIEDAD LIMITADA", "SOCIEDAD ANONIMA", "SOCIEDAD COOPERATIVA",
        "SOCIEDAD COMANDITARIA", "SOCIEDAD COLECTIVA", "COMUNIDAD DE BIENES",
        "SLU", "SLL", "SLP", "SLNE", "SAU", "SL", "SA", "SC", "SCOOP", "CB",
    ]:
        cleaned = re.sub(
            rf"\b{re.escape(suffix)}\b\.?\s*$", "", cleaned, flags=re.IGNORECASE
        ).strip()
    cleaned = cleaned.rstrip(".,- ")
    return cleaned


def _slug_from_name(nombre: str) -> str:
    """Convert company name to URL slug for direct lookup."""
    clean = _clean_name_full(nombre)
    slug = unidecode(clean).upper().replace(" ", "-")
    slug = re.sub(r"[^A-Z0-9\-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


async def _search_empresite(nombre: str) -> Optional[dict]:
    """Scrape Empresite (ElEconomista) for company data."""
    slug = _slug_from_name(nombre)
    url = f"https://empresite.eleconomista.es/{slug}.html"
    html = await _curl_fetch(url, timeout=10)
    if not html or "no encontrada" in html.lower() or "404" in html[:500]:
        return None

    if not _name_matches(nombre, html):
        return None

    result = {}
    cifs = CIF_RE.findall(html)
    if cifs:
        result["cif"] = Counter(cifs).most_common(1)[0][0]

    cnae_match = _CNAE_RE.search(html)
    if cnae_match:
        result["cnae_code"] = cnae_match.group(1)

    addr_match = _ADDR_RE.search(html)
    if addr_match:
        addr = re.sub(r"<[^>]+>", "", addr_match.group(1)).strip()
        if len(addr) > 10:
            result["domicilio"] = addr[:200]

    obj_match = _OBJ_RE.search(html)
    if obj_match:
        obj = re.sub(r"<[^>]+>", "", obj_match.group(1)).strip()
        if len(obj) > 15:
            result["objeto_social"] = obj[:300]

    return result if result.get("cif") else None


async def _search_infoempresa(nombre: str) -> Optional[dict]:
    """Scrape Infoempresa for company data."""
    slug = _slug_from_name(nombre).lower()
    url = f"https://www.infoempresa.com/es-es/es/empresa/{slug}"
    html = await _curl_fetch(url, timeout=10)
    if not html or len(html) < 1000:
        return None

    if not _name_matches(nombre, html):
        return None

    result = {}
    cifs = CIF_RE.findall(html)
    if cifs:
        result["cif"] = Counter(cifs).most_common(1)[0][0]

    cnae_match = _CNAE_RE.search(html)
    if cnae_match:
        result["cnae_code"] = cnae_match.group(1)

    return result if result.get("cif") else None


async def _search_einforma(nombre: str) -> Optional[dict]:
    """Scrape Einforma for company data."""
    slug = _slug_from_name(nombre).lower()
    url = f"https://www.einforma.com/informacion-empresa/{slug}"
    html = await _curl_fetch(url, timeout=10)
    if not html or len(html) < 1000:
        return None

    if not _name_matches(nombre, html):
        return None

    result = {}
    cifs = CIF_RE.findall(html)
    if cifs:
        result["cif"] = Counter(cifs).most_common(1)[0][0]

    cnae_match = _CNAE_RE.search(html)
    if cnae_match:
        result["cnae_code"] = cnae_match.group(1)

    return result if result.get("cif") else None


async def lookup_cif_by_name(nombre: str, **kwargs) -> Optional[str]:
    """Search free web sources for a company's CIF.

    Tries multiple sources: DuckDuckGo, Empresite, Infoempresa, Einforma.
    """
    # 1. DuckDuckGo (fast, broad)
    cif = await _search_ddg(nombre)
    if cif:
        return cif

    # 2. Direct lookup on business directories
    for source_fn in [_search_empresite, _search_infoempresa, _search_einforma]:
        try:
            result = await source_fn(nombre)
            if result and result.get("cif"):
                return result["cif"]
        except Exception as e:
            logger.debug(f"Source error for '{nombre}': {e}")
        await asyncio.sleep(1)

    return None


async def lookup_full_by_name(nombre: str) -> Optional[dict]:
    """Search for CIF + CNAE + address + objeto social.

    Returns dict with keys: cif, cnae_code, domicilio, objeto_social (all optional).
    """
    # 1. DuckDuckGo first for quick CIF
    cif = await _search_ddg(nombre)
    best = {"cif": cif} if cif else {}

    # 2. Try directories for richer data
    for source_fn in [_search_empresite, _search_einforma, _search_infoempresa]:
        try:
            result = await source_fn(nombre)
            if result:
                # Merge: keep first non-None value for each field
                for key in ("cif", "cnae_code", "domicilio", "objeto_social"):
                    if result.get(key) and not best.get(key):
                        best[key] = result[key]
                if best.get("cif"):
                    break  # Got CIF + extras, good enough
        except Exception as e:
            logger.debug(f"Source error for '{nombre}': {e}")
        await asyncio.sleep(1)

    return best if best.get("cif") else None


async def enrich_company_cif(company_id: int, db: AsyncSession) -> Optional[str]:
    """Lookup and store CIF (+ CNAE, address if available) for a single company."""
    company = await db.get(Company, company_id)
    if not company or company.cif:
        return company.cif if company else None

    result = await lookup_full_by_name(company.nombre)
    if result and result.get("cif"):
        company.cif = result["cif"]
        if result.get("cnae_code") and not company.cnae_code:
            company.cnae_code = result["cnae_code"]
        if result.get("domicilio") and not company.domicilio:
            company.domicilio = result["domicilio"]
        if result.get("objeto_social") and not company.objeto_social:
            company.objeto_social = result["objeto_social"]
        await db.commit()
        logger.info(f"CIF found: {company.nombre} -> {result['cif']}")
        return result["cif"]

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


async def count_cif_enrichable_filtered(db: AsyncSession, filters: dict) -> int:
    """Count companies matching filters that still need CIF enrichment."""
    MAX_INTENTOS = 2
    conditions = [Company.cif.is_(None), Company.cif_intentos < MAX_INTENTOS]
    if filters.get("provincia"):
        conditions.append(Company.provincia == filters["provincia"])
    if filters.get("cnae_code"):
        conditions.append(Company.cnae_code.startswith(filters["cnae_code"]))
    if filters.get("forma_juridica"):
        conditions.append(Company.forma_juridica == filters["forma_juridica"])
    if filters.get("estado"):
        conditions.append(Company.estado == filters["estado"])
    return await db.scalar(select(func.count(Company.id)).where(*conditions)) or 0
