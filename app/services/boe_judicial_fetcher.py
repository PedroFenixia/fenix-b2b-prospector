from __future__ import annotations

"""Fetch judicial notices from BOE (Section III - Administracion de Justicia).

Extracts: concursos de acreedores, embargos, procedimientos judiciales.
"""
import logging
import re
from datetime import date
from typing import Optional

import httpx
from lxml import etree

logger = logging.getLogger(__name__)

BOE_BASE = "https://www.boe.es"

# Keywords to classify judicial notice types
CONCURSO_KW = ["concurso", "concursal", "acreedores", "insolvencia", "liquidacion concursal"]
EMBARGO_KW = ["embargo", "ejecucion hipotecaria", "subasta", "adjudicacion"]


async def fetch_boe_judicial(fecha: date) -> list[dict]:
    """Fetch judicial notices from BOE for a given date.

    Parses Section III (Administracion de Justicia) of the BOE sumario.
    """
    url = f"{BOE_BASE}/datosabiertos/api/boe/sumario/{fecha.strftime('%Y%m%d')}"
    logger.info(f"Fetching BOE sumario for judicial notices: {url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"Accept": "application/xml"})

    if resp.status_code == 404:
        logger.info(f"No BOE published for {fecha}")
        return []

    if resp.status_code != 200:
        logger.error(f"BOE API error {resp.status_code} for {fecha}")
        return []

    items = _parse_judicial_from_sumario(fecha, resp.content)

    # Fetch detail for each item
    notices = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for item in items:
            detail = await _fetch_item_detail(client, item)
            if detail:
                notices.append(detail)

    logger.info(f"Found {len(notices)} judicial notices for {fecha}")
    return notices


def _parse_judicial_from_sumario(fecha: date, xml_content: bytes) -> list[dict]:
    """Parse BOE sumario XML for judicial notices.

    BOE section codes (attribute ``codigo``): "3" = Otras disposiciones,
    "4" = Administracion de Justicia (when published), "5B" = Otros anuncios
    oficiales. Judicial items can appear in any of these.

    Items can be children of ``<epigrafe>`` or direct children of
    ``<departamento>`` (section 5B has no epigrafes).
    """
    root = etree.fromstring(xml_content)
    items = []

    for seccion in root.iter("seccion"):
        codigo = seccion.get("codigo", "")
        # Check sections that may contain judicial notices
        if codigo not in ("3", "4", "5B"):
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

                # Determine epigrafe for classification
                parent = item.getparent()
                ep_name = parent.get("nombre", "") if parent is not None and parent.tag == "epigrafe" else ""

                # Classify type
                titulo_lower = titulo.lower()
                tipo = _classify_notice(titulo_lower, ep_name.lower())

                if item_id and titulo and tipo:
                    items.append({
                        "boe_id": item_id,
                        "titulo": titulo,
                        "tipo": tipo,
                        "juzgado": dept_name if dept_name else None,
                        "url_html": url_html,
                        "url_pdf": url_pdf,
                        "fecha_publicacion": fecha,
                    })

    return items


def _classify_notice(titulo: str, epigrafe: str) -> Optional[str]:
    """Classify a judicial notice by type based on title and epigraph."""
    combined = f"{titulo} {epigrafe}"

    if any(kw in combined for kw in CONCURSO_KW):
        return "concurso_acreedores"
    if any(kw in combined for kw in EMBARGO_KW):
        return "embargo"

    # Also include general judicial announcements from Juzgados Mercantiles
    if "juzgado" in combined and ("mercantil" in combined or "comercial" in combined):
        return "procedimiento_judicial"

    return None


async def _fetch_item_detail(client: httpx.AsyncClient, item: dict) -> Optional[dict]:
    """Fetch BOE document XML to extract full description and debtor info.

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
                desc = " ".join(full_text.split())[:3000]
                item["descripcion"] = desc

                # Try to extract debtor name
                deudor = _extract_deudor(desc)
                if deudor:
                    item["deudor"] = deudor

                # Try to extract location
                loc = _extract_localidad(desc)
                if loc:
                    item["localidad"] = loc.get("localidad")
                    item["provincia"] = loc.get("provincia")

        return item

    except Exception as e:
        logger.warning(f"Error fetching detail for {boe_id}: {e}")
        return item


def _extract_deudor(text: str) -> Optional[str]:
    """Try to extract debtor name from judicial notice text."""
    patterns = [
        r"(?:deudor|concursad[oa]|ejecutad[oa]|demandad[oa])\s*[:;]\s*(.+?)(?:\.|,|\n)",
        r"(?:contra|frente a)\s+(?:la empresa\s+|la mercantil\s+|D\.\s+|D\.\xaa\s+)?(.+?)(?:\.|,|\n)",
        r"(?:procedimiento concursal|concurso de acreedores)\s+(?:de\s+)?(.+?)(?:\.|,|\n)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            if len(name) > 5 and len(name) < 200:
                return name
    return None


def _extract_localidad(text: str) -> Optional[dict]:
    """Try to extract location from text."""
    m = re.search(
        r"(?:juzgado.*?de\s+)(\w[\w\s]+?)(?:\s*,\s*provincia\s+de\s+(\w[\w\s]+?))?(?:\.|,|\n)",
        text,
        re.IGNORECASE,
    )
    if m:
        result = {}
        if m.group(1):
            result["localidad"] = m.group(1).strip()[:100]
        if m.group(2):
            result["provincia"] = m.group(2).strip()[:50]
        return result if result else None
    return None
