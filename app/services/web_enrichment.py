"""Enriquecimiento de contacto: busca web, email y telefono en la web de la empresa.

Flujo mejorado:
1. Multi-busqueda en DuckDuckGo (2-4 queries en cascada)
2. Probar hasta 3 URLs corporativas (no solo la primera)
3. Verificar nombre con matching flexible por tokens
4. Rastrear hasta 5 paginas legales/contacto (priorizando contacto)
5. Extraer de texto, mailto:, tel:, meta tags y JSON-LD
6. Filtrar emails con lista de dominios spam

Nota: La busqueda de CIF se hace por separado en cif_enrichment.py.
"""
from __future__ import annotations

import asyncio
import json as _json
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

EMAIL_RE = re.compile(
    r"\b([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})\b"
)

# Telefono espanol: +34 o prefijo, 9 digitos
PHONE_RE = re.compile(
    r"(?:\+34[\s.\-]?)?(\d[\s.\-]?\d{2}[\s.\-]?\d{3}[\s.\-]?\d{3})\b"
    r"|(?:\+34[\s.\-]?)?(\d{3}[\s.\-]?\d{2}[\s.\-]?\d{2}[\s.\-]?\d{2})\b"
    r"|(?:\+34[\s.\-]?)?(\d{3}[\s.\-]?\d{3}[\s.\-]?\d{3})\b"
)

# Formas juridicas a eliminar para comparacion de nombres
LEGAL_FORMS = re.compile(
    r"\b(S\.?L\.?L?\.?|S\.?A\.?|S\.?C\.?|S\.?COOP\.?|S\.?L\.?U\.?|"
    r"S\.?A\.?U\.?|S\.?L\.?P\.?|SOCIEDAD LIMITADA|SOCIEDAD ANONIMA|"
    r"SOCIEDAD COOPERATIVA|SOCIEDAD CIVIL)\b",
    re.IGNORECASE,
)

# Sufijos de formas juridicas para limpiar queries de busqueda
_LEGAL_SUFFIXES = ["SL", "SLL", "SA", "SLU", "SAU", "SLNE", "SC", "SLP", "COOP", "CB"]

# Dominios a descartar en resultados de busqueda
SKIP_DOMAINS = {
    "facebook.com", "twitter.com", "linkedin.com", "instagram.com",
    "youtube.com", "tiktok.com", "wikipedia.org", "infocif.es",
    "einforma.com", "empresia.es", "axesor.es", "eleconomista.es",
    "expansion.com", "google.com", "bing.com", "amazon.com",
    "registradores.org", "boe.es", "libreborme.net", "x.com",
    "paginasamarillas.es", "yelp.es", "tripadvisor.es",
}

# Dominios de email a ignorar (third-party, tracking, etc.)
SKIP_EMAIL_DOMAINS = {
    "sentry.io", "googletagmanager.com", "google-analytics.com",
    "cookiebot.com", "cookieyes.com", "iubenda.com", "onetrust.com",
    "wordpress.org", "wordpress.com", "w3.org", "schema.org",
    "example.com", "test.com", "gravatar.com", "cloudflare.com",
    "wixpress.com", "squarespace.com", "mailchimp.com",
}

# Prefijos de email a ignorar
SKIP_EMAIL_PREFIXES = [
    "noreply", "no-reply", "no_reply", "mailer-daemon", "mailer",
    "postmaster", "tracking", "analytics", "unsubscribe", "bounce",
    "donotreply", "notifications", "newsletter", "wordpress",
]

# Paths de paginas legales/contacto
LEGAL_PATHS = [
    "contacto", "contact", "contacta",
    "about", "sobre-nosotros", "quienes-somos", "empresa",
    "aviso-legal", "aviso_legal", "avisolegal",
    "legal", "legal-notice",
    "politica-de-privacidad", "politica-privacidad", "privacidad", "privacy",
    "terminos", "condiciones", "terms",
    "imprint", "impressum",
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]


# --- Name helpers ---

def _normalize_name(name: str) -> str:
    """Normalize company name for comparison: remove legal form, accents, case."""
    name = LEGAL_FORMS.sub("", name)
    name = unidecode(name).upper().strip()
    name = re.sub(r"[^A-Z0-9\s]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _clean_search_name(nombre: str) -> str:
    """Remove legal form suffixes for cleaner search queries."""
    cleaned = nombre.strip()
    for suffix in _LEGAL_SUFFIXES:
        cleaned = re.sub(rf"\b{suffix}\b\.?$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = cleaned.rstrip(".,- ")
    return cleaned


def _names_match_flexible(borme_name: str, page_text_upper: str) -> bool:
    """Check if company name appears on page using flexible token matching.

    Strategy: split name into significant tokens (3+ chars), require that
    the longest token appears AND at least 60% of tokens match.
    """
    norm = _normalize_name(borme_name)
    if not norm:
        return False

    # Strategy 1: full normalized name appears
    if norm in page_text_upper:
        return True

    # Strategy 2: token overlap
    tokens = [t for t in norm.split() if len(t) >= 3]
    if not tokens:
        # Name is very short, try substring
        return norm in page_text_upper

    # Longest/most distinctive token MUST appear
    longest = max(tokens, key=len)
    if longest not in page_text_upper:
        return False

    # At least 60% of significant tokens must appear
    matches = sum(1 for t in tokens if t in page_text_upper)
    ratio = matches / len(tokens)
    return ratio >= 0.6


# --- Phone/email helpers ---

def _clean_phone(match: re.Match) -> str:
    """Extract clean phone number from regex match."""
    raw = match.group(0)
    digits = re.sub(r"[^\d+]", "", raw)
    if digits.startswith("+34"):
        digits = digits[3:]
    if len(digits) == 9 and digits[0] in "6789":
        return digits
    return ""


def _extract_emails_text(text: str) -> list[str]:
    """Extract emails from plain text."""
    emails = EMAIL_RE.findall(text)
    return [
        e for e in emails
        if not any(e.endswith(ext) for ext in [".png", ".jpg", ".gif", ".svg", ".webp"])
    ]


def _extract_phones_text(text: str) -> list[str]:
    """Extract phone numbers from plain text."""
    phones = []
    for m in PHONE_RE.finditer(text):
        clean = _clean_phone(m)
        if clean and clean not in phones:
            phones.append(clean)
    return phones


def _extract_from_html(html: str) -> tuple[list[str], list[str]]:
    """Extract emails and phones from HTML: text + attributes + structured data."""
    soup = BeautifulSoup(html, "html.parser")
    emails: list[str] = []
    phones: list[str] = []

    # 1. From text content
    text = soup.get_text(separator=" ", strip=True)
    emails.extend(_extract_emails_text(text))
    phones.extend(_extract_phones_text(text))

    # 2. From mailto: and tel: links
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("mailto:"):
            email = href.replace("mailto:", "").split("?")[0].strip()
            if EMAIL_RE.match(email) and email not in emails:
                emails.append(email)
        elif href.startswith("tel:"):
            raw_phone = href.replace("tel:", "").strip()
            digits = re.sub(r"[^\d]", "", raw_phone)
            if digits.startswith("34"):
                digits = digits[2:]
            if len(digits) == 9 and digits[0] in "6789" and digits not in phones:
                phones.append(digits)

    # 3. From meta tags
    for meta in soup.find_all("meta"):
        content = meta.get("content", "")
        if "@" in content:
            for match in EMAIL_RE.findall(content):
                if match not in emails:
                    emails.append(match)

    # 4. From JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = _json.loads(script.string or "")
            _extract_from_jsonld(data, emails, phones)
        except (_json.JSONDecodeError, TypeError):
            pass

    return emails, phones


def _extract_from_jsonld(data, emails: list, phones: list):
    """Extract contact info from JSON-LD structured data."""
    if isinstance(data, dict):
        for key in ("email", "contactEmail"):
            val = data.get(key, "")
            if val and EMAIL_RE.match(val) and val not in emails:
                emails.append(val)
        for key in ("telephone", "phone", "contactPhone"):
            val = data.get(key, "")
            if val:
                digits = re.sub(r"[^\d]", "", val)
                if digits.startswith("34"):
                    digits = digits[2:]
                if len(digits) == 9 and digits[0] in "6789" and digits not in phones:
                    phones.append(digits)
        for v in data.values():
            if isinstance(v, (dict, list)):
                _extract_from_jsonld(v, emails, phones)
    elif isinstance(data, list):
        for item in data:
            _extract_from_jsonld(item, emails, phones)


def _filter_emails(emails: list[str], company_domain: str | None = None) -> list[str]:
    """Filter out non-company emails (tracking, third-party, etc.)."""
    filtered = []
    for email in emails:
        lower = email.lower()
        domain = lower.split("@")[-1]

        # Skip known third-party domains
        if any(skip in domain for skip in SKIP_EMAIL_DOMAINS):
            continue
        # Skip noreply-type prefixes
        if any(lower.startswith(skip) for skip in SKIP_EMAIL_PREFIXES):
            continue

        filtered.append(email)

    # Prioritize emails from the company domain
    if company_domain:
        cd = company_domain.lower()
        company_emails = [e for e in filtered if cd in e.lower()]
        other_emails = [e for e in filtered if cd not in e.lower()]
        return company_emails + other_emails

    return filtered


# --- HTTP / Search ---

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


async def _search_ddg(query: str) -> list[str]:
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


async def _search_multi_strategy(nombre: str, provincia: str | None = None) -> list[str]:
    """Try multiple search queries in cascade until we get 3+ corporate URLs."""
    clean = _clean_search_name(nombre)

    strategies = [
        f'"{clean}" empresa España',
        f'{clean} web oficial contacto',
    ]
    if provincia:
        strategies.append(f'{clean} {provincia} empresa')
    strategies.append(f'site:.es {clean}')

    all_urls: list[str] = []
    for query in strategies:
        urls = await _search_ddg(query.strip())
        corporate = [u for u in urls if _is_corporate_url(u)]
        all_urls.extend(u for u in corporate if u not in all_urls)
        if len(all_urls) >= 3:
            break
        await asyncio.sleep(random.uniform(1.0, 2.0))

    return all_urls


def _is_corporate_url(url: str) -> bool:
    """Check if URL looks like a corporate website (not a directory/social)."""
    domain = urlparse(url).netloc.lower().replace("www.", "")
    return not any(skip in domain for skip in SKIP_DOMAINS)


async def _fetch_page(url: str, client: httpx.AsyncClient = None) -> Optional[str]:
    """Fetch a web page. Tries curl first (better TLS fingerprint), falls back to httpx."""
    # Try curl first
    html = await _curl_fetch(url, timeout=10)
    if html:
        lower = html[:500].lower()
        if "<html" in lower or "<head" in lower or "<!doctype" in lower:
            return html

    # Fallback to httpx
    if client:
        try:
            resp = await client.get(
                url,
                headers={"User-Agent": random.choice(_USER_AGENTS)},
                follow_redirects=True,
                timeout=10.0,
            )
            if resp.status_code == 200 and "text/html" in resp.headers.get("content-type", ""):
                return resp.text
        except Exception:
            pass

    return None


def _find_legal_links(html: str, base_url: str) -> list[str]:
    """Find links to legal/privacy/contact pages, prioritizing contact."""
    soup = BeautifulSoup(html, "html.parser")
    contact_urls: list[str] = []
    other_urls: list[str] = []

    contact_keywords = {"contacto", "contact", "contacta", "about", "quienes-somos", "sobre-nosotros", "empresa"}

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(strip=True).lower()

        is_legal = any(path in href for path in LEGAL_PATHS) or any(
            kw in text
            for kw in [
                "aviso legal", "legal", "privacidad", "privacy",
                "contacto", "contact", "condiciones", "términos",
                "sobre nosotros", "quienes somos", "empresa",
            ]
        )
        if is_legal:
            full_url = urljoin(base_url, a["href"])
            is_contact = any(kw in href or kw in text for kw in contact_keywords)
            if is_contact and full_url not in contact_urls:
                contact_urls.append(full_url)
            elif full_url not in contact_urls and full_url not in other_urls:
                other_urls.append(full_url)

    return (contact_urls + other_urls)[:5]


# --- Main enrichment function ---

async def enrich_company_web(
    company: Company,
    client: httpx.AsyncClient,
) -> dict:
    """Enrich a single company with contact data (web, email, phone).

    Improved flow:
    1. Multi-strategy search (2-4 queries)
    2. Try up to 3 corporate URLs
    3. Flexible name matching (token-based)
    4. Fetch up to 5 legal/contact pages
    5. Rich extraction (text + HTML attrs + JSON-LD)
    6. Smart email filtering

    Returns dict with keys: email, telefono, web (or None for each).
    """
    result = {"email": None, "telefono": None, "web": None}
    nombre = company.nombre
    provincia = getattr(company, "provincia", None)

    # 1. Multi-strategy search
    search_urls = await _search_multi_strategy(nombre, provincia)
    if not search_urls:
        return result

    # 2. Try up to 3 corporate URLs with flexible name matching
    corporate_url = None
    homepage_html = None

    for candidate_url in search_urls[:3]:
        html = await _fetch_page(candidate_url, client)
        if not html:
            continue
        page_text_upper = unidecode(
            BeautifulSoup(html, "html.parser").get_text(separator=" ", strip=True)
        ).upper()
        if _names_match_flexible(nombre, page_text_upper):
            corporate_url = candidate_url
            homepage_html = html
            break
        await asyncio.sleep(0.3)

    if not corporate_url or not homepage_html:
        return result

    # Web confirmed as belonging to the company
    result["web"] = corporate_url
    company_domain = urlparse(corporate_url).netloc.lower().replace("www.", "")

    # Collect all HTML pages to analyze
    all_htmls = [homepage_html]

    # 3. Find and fetch contact/legal pages (up to 5, prioritizing contacto)
    legal_links = _find_legal_links(homepage_html, corporate_url)
    for link in legal_links:
        legal_html = await _fetch_page(link, client)
        if legal_html:
            all_htmls.append(legal_html)
        await asyncio.sleep(0.3)

    # 4. Extract email and phone from all pages (text + HTML attributes + JSON-LD)
    all_emails: list[str] = []
    all_phones: list[str] = []

    for html in all_htmls:
        page_emails, page_phones = _extract_from_html(html)
        all_emails.extend(e for e in page_emails if e not in all_emails)
        all_phones.extend(p for p in page_phones if p not in all_phones)

    # 5. Filter and select best email
    filtered_emails = _filter_emails(all_emails, company_domain)
    if filtered_emails:
        # Prefer info@, contacto@, etc.
        for prefix in ["info", "contacto", "contact", "hola", "admin"]:
            for email in filtered_emails:
                if email.lower().startswith(prefix):
                    result["email"] = email
                    break
            if result["email"]:
                break
        if not result["email"]:
            result["email"] = filtered_emails[0]

    # 6. First valid phone
    if all_phones:
        result["telefono"] = all_phones[0]

    return result


# --- Batch and single enrichment ---

async def enrich_batch_web(
    db: AsyncSession,
    limit: int = 20,
) -> dict:
    """Enrich a batch of companies via web search. Returns stats dict."""
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


async def count_enrichable_filtered(db: AsyncSession, filters: dict) -> int:
    """Count companies matching filters that can still be enriched."""
    from sqlalchemy import func as f

    conditions = [
        Company.web.is_(None),
        Company.web_intentos < 2,
    ]
    if filters.get("estado"):
        conditions.append(Company.estado == filters["estado"])
    if filters.get("provincia"):
        conditions.append(Company.provincia == filters["provincia"])
    if filters.get("cnae_code"):
        conditions.append(Company.cnae_code.startswith(filters["cnae_code"]))
    if filters.get("forma_juridica"):
        conditions.append(Company.forma_juridica == filters["forma_juridica"])

    count = await db.scalar(select(f.count(Company.id)).where(*conditions))
    return count or 0
