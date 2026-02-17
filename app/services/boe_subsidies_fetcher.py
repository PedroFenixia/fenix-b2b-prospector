from __future__ import annotations

"""Fetch subsidies (subvenciones/ayudas) from BOE open data API.

BOE publishes subsidies in Sección V.B (Otros anuncios oficiales - Subvenciones).
The API returns an XML sumario with items linking to each publication.
"""
import logging
import re
from datetime import date
from typing import Optional

import httpx
from lxml import etree

from app.config import settings

logger = logging.getLogger(__name__)

BOE_BASE = "https://www.boe.es"


async def fetch_boe_subsidies(fecha: date) -> list[dict]:
    """Fetch subsidies published on a given date from BOE sumario.

    Returns list of dicts with subsidy data ready for DB insertion.
    """
    url = f"{BOE_BASE}/datosabiertos/api/boe/sumario/{fecha.strftime('%Y%m%d')}"
    logger.info(f"Fetching BOE sumario for subsidies: {url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"Accept": "application/xml"})

    if resp.status_code == 404:
        logger.info(f"No BOE published for {fecha}")
        return []

    if resp.status_code != 200:
        logger.error(f"BOE API error {resp.status_code} for {fecha}")
        return []

    items = _parse_subsidies_from_sumario(fecha, resp.content)

    # Fetch detail for each item to get full description
    subsidies = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for item in items:
            detail = await _fetch_item_detail(client, item)
            if detail:
                subsidies.append(detail)

    logger.info(f"Found {len(subsidies)} subsidies for {fecha}")
    return subsidies


def _parse_subsidies_from_sumario(fecha: date, xml_content: bytes) -> list[dict]:
    """Parse BOE sumario XML to extract subsidy items.

    BOE section codes (attribute ``codigo``): "3" = Otras disposiciones,
    "5A" = Contratacion del Sector Publico, "5B" = Otros anuncios oficiales.
    Subsidies appear mainly in 3 and 5B.

    Items can be children of ``<epigrafe>`` or direct children of
    ``<departamento>`` (section 5B has no epigrafes).
    """
    root = etree.fromstring(xml_content)
    items = []

    # Keywords that indicate a subsidy/grant
    SUBSIDY_KW = [
        "subvenci", "ayudas", "bases reguladora",
        "becas", "financiaci", "incentivo",
        "concesión directa", "línea de ayuda",
    ]
    # False-positive exclusions
    EXCLUDE_KW = [
        "hidrogr", "portuaria", "confederación",
        "regantes", "junta general", "asamblea",
        "convocatoria de junta", "convocatoria de sesión",
        "mutualidad", "funcionarios civiles",
    ]

    for seccion in root.iter("seccion"):
        codigo = seccion.get("codigo", "")
        # Subsidies appear in sections 3, 5A, and 5B
        if not (codigo.startswith("5") or codigo == "3"):
            continue

        for departamento in seccion.findall(".//departamento"):
            dept_name = departamento.get("nombre", "")

            # Items can be under <epigrafe> or directly under <departamento>
            all_items = departamento.findall(".//item")

            for item in all_items:
                id_elem = item.find("identificador")
                item_id = id_elem.text.strip() if id_elem is not None and id_elem.text else ""

                titulo_elem = item.find("titulo")
                titulo = titulo_elem.text.strip() if titulo_elem is not None and titulo_elem.text else ""

                url_html_elem = item.find("url_html")
                url_html = ""
                if url_html_elem is not None and url_html_elem.text:
                    url_html = url_html_elem.text.strip()

                url_pdf_elem = item.find("url_pdf")
                url_pdf = ""
                if url_pdf_elem is not None and url_pdf_elem.text:
                    url_pdf = url_pdf_elem.text.strip()

                # Determine epigrafe (parent may be epigrafe or departamento)
                parent = item.getparent()
                ep_name = parent.get("nombre", "") if parent is not None and parent.tag == "epigrafe" else ""

                titulo_lower = titulo.lower()
                is_subsidy = any(kw in titulo_lower for kw in SUBSIDY_KW)

                # Exclude false positives (water concessions, port authority, etc.)
                if is_subsidy and any(ex in titulo_lower for ex in EXCLUDE_KW):
                    is_subsidy = False

                if item_id and titulo and is_subsidy:
                    items.append({
                        "boe_id": item_id,
                        "titulo": titulo,
                        "organismo": dept_name,
                        "url_html": url_html,
                        "url_pdf": url_pdf,
                        "fecha_publicacion": fecha,
                        "sector": ep_name if ep_name else None,
                    })

    return items


async def _fetch_item_detail(client: httpx.AsyncClient, item: dict) -> Optional[dict]:
    """Fetch BOE document XML to get full description and metadata.

    The /api/boe/documento/ endpoint does NOT exist. Instead we use
    /diario_boe/xml.php?id={BOE_ID} which returns the full document XML.
    """
    boe_id = item["boe_id"]
    url = f"{BOE_BASE}/diario_boe/xml.php?id={boe_id}"

    try:
        resp = await client.get(url)
        if resp.status_code != 200:
            return item

        root = etree.fromstring(resp.content)

        # Some BOE-B documents return <error> instead of <documento>
        if root.tag == "error":
            logger.debug(f"BOE XML API returned error for {boe_id}, skipping detail")
            return item

        # <texto> may contain child elements (p, table, etc.)
        texto_elem = root.find(".//texto")
        if texto_elem is not None:
            full_text = etree.tostring(texto_elem, method="text", encoding="unicode")
            if full_text:
                item["descripcion"] = " ".join(full_text.split())[:2000]

        # Try to extract importe (amount)
        titulo_text = item.get("titulo", "") + " " + item.get("descripcion", "")
        importe = _extract_importe(titulo_text)
        if importe:
            item["importe"] = importe

        # Extract ambito (geographic scope)
        ambito_elem = root.find(".//ambito_geografico")
        if ambito_elem is not None and ambito_elem.text:
            item["ambito"] = ambito_elem.text.strip()

        # Extract materias (topics/beneficiaries)
        for materia in root.findall(".//materia"):
            if materia.text and materia.text.strip():
                item["beneficiarios"] = materia.text.strip()[:500]
                break

        # Extract comunidad_autonoma and provincia
        from app.services.geo_sector import detect_ccaa_from_text, detect_provincia_from_text
        combined = f"{item.get('organismo', '')} {item.get('ambito', '')} {item.get('titulo', '')}"
        ccaa = detect_ccaa_from_text(combined)
        if ccaa:
            item["comunidad_autonoma"] = ccaa
        prov = detect_provincia_from_text(combined)
        if prov:
            item["provincia"] = prov

        # Detect CNAE from text
        cnae = _detect_cnae_from_text(f"{item.get('titulo', '')} {item.get('descripcion', '')}")
        if cnae:
            item["cnae_codes"] = cnae

        return item

    except Exception as e:
        logger.warning(f"Error fetching detail for {boe_id}: {e}")
        return item


# Keyword -> CNAE code mapping for subsidies
_SUBSIDY_CNAE_MAP = [
    (["agricultur", "agrari", "ganad", "pesca", "forestal"], "01"),
    (["industria", "manufactur", "fabricación"], "10"),
    (["construcción", "edificación", "obra"], "41"),
    (["transporte", "logística", "movilidad"], "49"),
    (["turismo", "hostelería", "alojamiento"], "55"),
    (["tecnología", "digital", "informátic", "software", "TIC"], "62"),
    (["investigación", "I+D", "innovación", "ciencia"], "72"),
    (["educación", "formación", "enseñanza"], "85"),
    (["sanidad", "salud", "sanitari", "médic", "farmac"], "86"),
    (["energía", "renovable", "eficiencia energética"], "35"),
    (["comercio", "exportación", "internacionalización"], "46"),
    (["cultura", "patrimonio", "artístic"], "90"),
    (["medio ambiente", "residuo", "reciclaje", "sostenib"], "38"),
    (["empleo", "contratación", "autónomo", "emprendim"], "78"),
    (["vivienda", "rehabilitación", "accesibilidad"], "41"),
]


def _detect_cnae_from_text(text: str) -> Optional[str]:
    """Detect CNAE codes from subsidy text content."""
    if not text:
        return None
    text_lower = text.lower()
    codes = []
    for keywords, cnae in _SUBSIDY_CNAE_MAP:
        if any(kw.lower() in text_lower for kw in keywords):
            if cnae not in codes:
                codes.append(cnae)
    return ",".join(codes) if codes else None


def _extract_importe(text: str) -> Optional[float]:
    """Try to extract monetary amount from text."""
    if not text:
        return None
    # Match patterns like "1.234.567,89 euros" or "1.234.567 €"
    patterns = [
        r'(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)\s*(?:euros|€|EUR)',
        r'importe.*?(\d{1,3}(?:\.\d{3})*(?:,\d{2})?)',
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            amount_str = m.group(1).replace(".", "").replace(",", ".")
            try:
                return float(amount_str)
            except ValueError:
                continue
    return None
