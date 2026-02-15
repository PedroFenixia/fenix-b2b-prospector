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
    """Parse BOE sumario XML to extract subsidy items from Section V.B.

    BOE structure: sumario > diario > seccion[@num='5'] > departamento > epigrafe > item
    Section 5 = "V. Anuncios", subsections B/C contain grants/subsidies.
    """
    root = etree.fromstring(xml_content)
    items = []

    for seccion in root.iter("seccion"):
        num = seccion.get("num", "")
        # Section 5 = V. Anuncios (contains subsidies in subsections B, C)
        if num != "5":
            continue

        for departamento in seccion.findall(".//departamento"):
            dept_name = departamento.get("nombre", "")

            for epigrafe in departamento.findall(".//epigrafe"):
                ep_name = epigrafe.get("nombre", "")

                for item in epigrafe.findall("item"):
                    item_id = item.get("id", "")
                    titulo_elem = item.find("titulo")
                    titulo = titulo_elem.text.strip() if titulo_elem is not None and titulo_elem.text else ""

                    url_html_elem = item.find("url_html")
                    url_html = ""
                    if url_html_elem is not None and url_html_elem.text:
                        url_html = url_html_elem.text.strip()
                        if url_html and not url_html.startswith("http"):
                            url_html = f"{BOE_BASE}{url_html}"

                    url_pdf_elem = item.find("url_pdf")
                    url_pdf = ""
                    if url_pdf_elem is not None and url_pdf_elem.text:
                        url_pdf = url_pdf_elem.text.strip()
                        if url_pdf and not url_pdf.startswith("http"):
                            url_pdf = f"{BOE_BASE}{url_pdf}"

                    # Only include items that look like subsidies/grants
                    titulo_lower = titulo.lower()
                    is_subsidy = any(kw in titulo_lower for kw in [
                        "subvenci", "ayuda", "convocatoria", "bases reguladora",
                        "concesi", "becas", "financiaci", "incentivo",
                    ])

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
    """Fetch BOE item detail API to get full description and metadata."""
    boe_id = item["boe_id"]
    url = f"{BOE_BASE}/datosabiertos/api/boe/documento/{boe_id}"

    try:
        resp = await client.get(url, headers={"Accept": "application/xml"})
        if resp.status_code != 200:
            return item  # Return basic info if detail unavailable

        root = etree.fromstring(resp.content)

        # Extract description from <texto> or <titulo>
        texto_elem = root.find(".//texto")
        if texto_elem is not None and texto_elem.text:
            item["descripcion"] = texto_elem.text.strip()[:2000]

        # Try to extract importe (amount)
        titulo_text = item.get("titulo", "") + " " + item.get("descripcion", "")
        importe = _extract_importe(titulo_text)
        if importe:
            item["importe"] = importe

        # Extract ambito (geographic scope)
        ambito_elem = root.find(".//ambito_geografico")
        if ambito_elem is not None and ambito_elem.text:
            item["ambito"] = ambito_elem.text.strip()

        # Try to find deadline info
        materias = root.find(".//materias")
        if materias is not None and materias.text:
            item["beneficiarios"] = materias.text.strip()[:500]

        return item

    except Exception as e:
        logger.warning(f"Error fetching detail for {boe_id}: {e}")
        return item


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
