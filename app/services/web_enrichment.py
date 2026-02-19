"""Enriquecimiento de contacto: busca web, email y teléfono en la web de la empresa.

Flujo:
1. Buscar el nombre de la empresa en Bing
2. Identificar la web corporativa (descartando directorios, redes sociales)
3. Buscar páginas legales (aviso legal, política de privacidad, contacto)
4. Extraer email y teléfono
5. Verificar que el nombre coincide (sin forma jurídica, case-insensitive)

Nota: La búsqueda de CIF se hace por separado en cif_enrichment.py.
"""
from __future__ import annotations

import asyncio
import logging
import random
import re
from typing import Optional
from urllib.parse import urljoin, urlparse, urlencode

import httpx
from bs4 import BeautifulSoup
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from unidecode import unidecode

from app.db.models import Company

logger = logging.getLogger(__name__)

# --- Regex patterns ---

# Email
EMAIL_RE = re.compile(
    r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b"
)

# Teléfono español: +34 o prefijo, 9 dígitos
PHONE_RE = re.compile(
    r"(?:\+34[\s.\-]?)?(\d[\s.\-]?\d{2}[\s.\-]?\d{3}[\s.\-]?\d{3})\b"
    r"|(?:\+34[\s.\-]?)?(\d{3}[\s.\-]?\d{2}[\s.\-]?\d{2}[\s.\-]?\d{2})\b"
    r"|(?:\+34[\s.\-]?)?(\d{3}[\s.\-]?\d{3}[\s.\-]?\d{3})\b"
)

# Formas jurídicas a eliminar para comparación de nombres
LEGAL_FORMS = re.compile(
    r"\b(S\.?L\.?L?\.?|S\.?A\.?|S\.?C\.?|S\.?COOP\.?|S\.?L\.?U\.?|"
    r"S\.?A\.?U\.?|S\.?L\.?P\.?|SOCIEDAD LIMITADA|SOCIEDAD ANONIMA|"
    r"SOCIEDAD COOPERATIVA|SOCIEDAD CIVIL)\b",
    re.IGNORECASE,
)

# Dominios a descartar en resultados de búsqueda
SKIP_DOMAINS = {
    "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
    "youtube.com", "tiktok.com", "wikipedia.org", "infocif.es",
    "einforma.com", "empresia.es", "axesor.es", "eleconomista.es",
    "expansion.com", "google.com", "bing.com", "amazon.com",
    "registradores.org", "boe.es", "libreborme.net",
}

# Paths de páginas legales donde suele estar el CIF
LEGAL_PATHS = [
    "aviso-legal", "aviso_legal", "avisolegal",
    "legal", "legal-notice",
    "politica-de-privacidad", "politica-privacidad", "privacidad", "privacy",
    "terminos", "condiciones", "terms",
    "contacto", "contact",
    "about", "sobre-nosotros", "quienes-somos",
    "imprint", "impressum",
]

# User-Agent realista
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _normalize_name(name: str) -> str:
    """Normalize company name for comparison: remove legal form, accents, case."""
    name = LEGAL_FORMS.sub("", name)
    name = unidecode(name).upper().strip()
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _names_match(borme_name: str, web_name: str) -> bool:
    """Check if names match (one contains the other after normalization)."""
    n1 = _normalize_name(borme_name)
    n2 = _normalize_name(web_name)
    if not n1 or not n2:
        return False
    # Exact or one contains the other
    return n1 == n2 or n1 in n2 or n2 in n1


def _clean_phone(match: re.Match) -> str:
    """Extract clean phone number from regex match."""
    raw = match.group(0)
    digits = re.sub(r"[^\d+]", "", raw)
    if digits.startswith("+34"):
        digits = digits[3:]
    if len(digits) == 9 and digits[0] in "6789":
        return digits
    return ""


def _extract_emails(text: str) -> list[str]:
    emails = EMAIL_RE.findall(text)
    # Filter out image/file emails
    return [
        e for e in emails
        if not any(e.endswith(ext) for ext in [".png", ".jpg", ".gif", ".svg", ".webp"])
    ]


def _extract_phones(text: str) -> list[str]:
    phones = []
    for m in PHONE_RE.finditer(text):
        clean = _clean_phone(m)
        if clean and clean not in phones:
            phones.append(clean)
    return phones


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


async def _curl_fetch(url: str, timeout: int = 10) -> Optional[str]:
    """Fetch URL using curl subprocess (bypasses TLS fingerprinting)."""
    try:
        ua = random.choice(_USER_AGENTS)
        cmd = [
            "curl", "-s", "-L",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {ua}",
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


async def _search_ddg(query: str, client: httpx.AsyncClient = None) -> list[str]:
    """Search DuckDuckGo HTML and extract result URLs using curl."""
    url = f"https://html.duckduckgo.com/html/?{urlencode({'q': query})}"
    try:
        html = await _curl_fetch(url)
        if not html:
            return []

        soup = BeautifulSoup(html, "html.parser")
        urls = []
        for a in soup.find_all("a", class_="result__a", href=True):
            href = a["href"]
            if href.startswith("http") and "duckduckgo.com" not in href:
                if href not in urls:
                    urls.append(href)
        if not urls:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "duckduckgo.com" not in href and "search" not in href[:30]:
                    if href not in urls:
                        urls.append(href)
        return urls[:10]
    except Exception as e:
        logger.warning(f"DDG search error: {e}")
        return []


def _is_corporate_url(url: str) -> bool:
    """Check if URL looks like a corporate website (not a directory/social)."""
    domain = urlparse(url).netloc.lower().replace("www.", "")
    return not any(skip in domain for skip in SKIP_DOMAINS)


async def _fetch_page(url: str, client: httpx.AsyncClient) -> Optional[str]:
    """Fetch a web page, return HTML text or None."""
    try:
        resp = await client.get(
            url,
            headers={"User-Agent": UA},
            follow_redirects=True,
            timeout=10.0,
        )
        if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
            return resp.text
    except Exception:
        pass
    return None


def _find_legal_links(html: str, base_url: str) -> list[str]:
    """Find links to legal/privacy/contact pages."""
    soup = BeautifulSoup(html, "html.parser")
    legal_urls = []

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(strip=True).lower()

        is_legal = any(path in href for path in LEGAL_PATHS) or any(
            kw in text
            for kw in [
                "aviso legal", "legal", "privacidad", "privacy",
                "contacto", "contact", "condiciones", "términos",
            ]
        )
        if is_legal:
            full_url = urljoin(base_url, a["href"])
            if full_url not in legal_urls:
                legal_urls.append(full_url)

    return legal_urls[:5]


async def enrich_company_web(
    company: Company,
    client: httpx.AsyncClient,
) -> dict:
    """Enrich a single company with contact data (web, email, phone).

    CIF lookup is handled separately by cif_enrichment.py.
    Returns dict with keys: email, telefono, web (or None for each).
    """
    result = {"email": None, "telefono": None, "web": None}
    nombre = company.nombre

    # 1. Search Brave
    search_urls = await _search_ddg(f"{nombre} empresa España", client)
    if not search_urls:
        return result

    # 2. Find first corporate URL
    corporate_url = None
    for url in search_urls:
        if _is_corporate_url(url):
            corporate_url = url
            break

    if not corporate_url:
        return result

    # 3. Fetch homepage
    homepage_html = await _fetch_page(corporate_url, client)
    if not homepage_html:
        return result

    # 4. Verify the website belongs to this company (name must appear on the page)
    homepage_text = BeautifulSoup(homepage_html, "html.parser").get_text(separator=" ", strip=True)
    homepage_text_upper = unidecode(homepage_text).upper()
    norm_name = _normalize_name(nombre)

    if not norm_name or norm_name[:15] not in homepage_text_upper:
        logger.info(f"[WebEnrich] {nombre}: web {corporate_url} does not mention company name, skipping")
        return result

    # Web confirmed as belonging to the company
    result["web"] = corporate_url

    # Collect all text to analyze: homepage + contact/legal pages
    all_texts = [homepage_html]

    # 5. Find and fetch contact/legal pages (max 2 to keep it fast)
    legal_links = _find_legal_links(homepage_html, corporate_url)
    for link in legal_links[:2]:
        legal_html = await _fetch_page(link, client)
        if legal_html:
            all_texts.append(legal_html)
        await asyncio.sleep(0.3)

    # 6. Extract email and phone from all pages
    all_emails = []
    all_phones = []

    for html in all_texts:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        all_emails.extend(_extract_emails(text))
        all_phones.extend(_extract_phones(text))

    # 7. Best email (prefer info@, contacto@, not noreply@)
    if all_emails:
        unique_emails = list(dict.fromkeys(all_emails))
        for prefix in ["info", "contacto", "contact", "hola", "admin"]:
            for email in unique_emails:
                if email.lower().startswith(prefix):
                    result["email"] = email
                    break
            if result["email"]:
                break
        if not result["email"]:
            for email in unique_emails:
                if not any(skip in email.lower() for skip in ["noreply", "no-reply", "mailer", "tracking", "analytics"]):
                    result["email"] = email
                    break

    # 8. First valid phone
    if all_phones:
        result["telefono"] = all_phones[0]

    return result


async def enrich_batch_web(
    db: AsyncSession,
    limit: int = 20,
) -> dict:
    """Enrich a batch of companies via web search.

    Returns stats dict.
    """
    # Companies without web, CIF, email, or phone - prioritize recent
    companies = (
        await db.scalars(
            select(Company)
            .where(
                Company.web.is_(None),
                Company.estado == "activa",
            )
            .order_by(Company.fecha_ultima_publicacion.desc())
            .limit(limit)
        )
    ).all()

    stats = {"attempted": 0, "web_found": 0, "email_found": 0, "phone_found": 0}

    async with httpx.AsyncClient(timeout=15.0) as client:
        for company in companies:
            stats["attempted"] += 1
            try:
                result = await enrich_company_web(company, client)

                if result["web"]:
                    company.web = result["web"]
                    stats["web_found"] += 1
                if result["email"]:
                    company.email = result["email"]
                    stats["email_found"] += 1
                if result["telefono"]:
                    company.telefono = result["telefono"]
                    stats["phone_found"] += 1

                logger.info(
                    f"[WebEnrich] {company.nombre}: "
                    f"web={result['web'] is not None}, "
                    f"email={result['email'] is not None}, tel={result['telefono'] is not None}"
                )
            except Exception as e:
                logger.error(f"[WebEnrich] Error for {company.nombre}: {e}")

            # Rate limit: 3s between companies to avoid Bing blocks
            await asyncio.sleep(3)

    await db.commit()
    return stats


async def enrich_single_web(
    company_id: int,
    db: AsyncSession,
) -> dict:
    """Enrich a single company via web search (web, email, phone only)."""
    company = await db.get(Company, company_id)
    if not company:
        return {"error": "Empresa no encontrada"}

    async with httpx.AsyncClient(timeout=15.0) as client:
        result = await enrich_company_web(company, client)

    if result["web"]:
        company.web = result["web"]
    if result["email"]:
        company.email = result["email"]
    if result["telefono"]:
        company.telefono = result["telefono"]

    await db.commit()
    return result


async def count_web_coverage(db: AsyncSession) -> dict:
    """Get web enrichment coverage stats."""
    from sqlalchemy import func as f
    total = await db.scalar(select(f.count(Company.id)))
    with_web = await db.scalar(select(f.count(Company.id)).where(Company.web.isnot(None)))
    with_email = await db.scalar(select(f.count(Company.id)).where(Company.email.isnot(None)))
    with_phone = await db.scalar(select(f.count(Company.id)).where(Company.telefono.isnot(None)))
    return {
        "total": total or 0,
        "with_web": with_web or 0,
        "with_email": with_email or 0,
        "with_phone": with_phone or 0,
    }
