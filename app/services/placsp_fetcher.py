from __future__ import annotations

"""Fetch public tenders from Plataforma de Contratación del Sector Público.

PLACSP provides ATOM feeds with tender data following CODICE standard.
Main feed: https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerique/
Also provides monthly summary feeds.
"""
import logging
import re
from datetime import date, datetime
from typing import Optional

import httpx
from lxml import etree

logger = logging.getLogger(__name__)

# ATOM feed URLs
PLACSP_FEED_URL = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilContratanteComplworte3.atom"
PLACSP_RECENT_URL = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilContratanteComplworte3.atom"

# XML namespaces used in PLACSP ATOM feeds
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "cbc": "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2",
    "cac": "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2",
    "cbc-place": "urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2",
}


async def fetch_recent_tenders(max_entries: int = 100) -> list[dict]:
    """Fetch recent tenders from PLACSP ATOM feed.

    Returns list of dicts ready for DB insertion.
    """
    logger.info("Fetching PLACSP recent tenders feed")

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(
            PLACSP_FEED_URL,
            headers={"Accept": "application/atom+xml, application/xml, text/xml"},
        )

    if resp.status_code != 200:
        logger.error(f"PLACSP feed error: {resp.status_code}")
        return []

    tenders = _parse_atom_feed(resp.content, max_entries)
    logger.info(f"Parsed {len(tenders)} tenders from PLACSP feed")
    return tenders


async def fetch_tenders_by_search(
    keyword: str = "",
    tipo_contrato: str = "",
    max_entries: int = 50,
) -> list[dict]:
    """Search PLACSP using the public search API.

    Uses the public search endpoint to find specific tenders.
    """
    search_url = "https://contrataciondelestado.es/sindicacion/sindicacion_643/licitacionesPerfilContratanteComplworte3.atom"
    params = {}
    if keyword:
        params["lici_nombre"] = keyword
    if tipo_contrato:
        params["lici_tipo"] = tipo_contrato

    logger.info(f"Searching PLACSP tenders: keyword={keyword}, tipo={tipo_contrato}")

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(
            search_url,
            params=params,
            headers={"Accept": "application/atom+xml, application/xml, text/xml"},
        )

    if resp.status_code != 200:
        logger.error(f"PLACSP search error: {resp.status_code}")
        return []

    return _parse_atom_feed(resp.content, max_entries)


def _parse_atom_feed(xml_content: bytes, max_entries: int = 100) -> list[dict]:
    """Parse PLACSP ATOM feed into tender dicts.

    The ATOM feed has <entry> elements containing tender data in CODICE XML format.
    """
    tenders = []

    try:
        root = etree.fromstring(xml_content)
    except etree.XMLSyntaxError as e:
        logger.error(f"Failed to parse PLACSP XML: {e}")
        return []

    # Get all namespaces from the document
    nsmap = {}
    for elem in root.iter():
        nsmap.update({k: v for k, v in elem.nsmap.items() if k is not None})

    # ATOM namespace
    atom_ns = "http://www.w3.org/2005/Atom"
    entries = root.findall(f"{{{atom_ns}}}entry")

    for entry in entries[:max_entries]:
        tender = _parse_entry(entry, atom_ns, nsmap)
        if tender:
            tenders.append(tender)

    return tenders


def _parse_entry(entry, atom_ns: str, nsmap: dict) -> Optional[dict]:
    """Parse a single ATOM entry into a tender dict."""
    # Basic ATOM fields
    id_elem = entry.find(f"{{{atom_ns}}}id")
    title_elem = entry.find(f"{{{atom_ns}}}title")
    updated_elem = entry.find(f"{{{atom_ns}}}updated")
    summary_elem = entry.find(f"{{{atom_ns}}}summary")
    link_elem = entry.find(f"{{{atom_ns}}}link[@rel='alternate']")
    if link_elem is None:
        link_elem = entry.find(f"{{{atom_ns}}}link")

    entry_id = id_elem.text.strip() if id_elem is not None and id_elem.text else ""
    titulo = title_elem.text.strip() if title_elem is not None and title_elem.text else ""
    descripcion = summary_elem.text.strip() if summary_elem is not None and summary_elem.text else ""
    url = link_elem.get("href", "") if link_elem is not None else ""

    if not entry_id or not titulo:
        return None

    # Parse date
    fecha_pub = date.today()
    if updated_elem is not None and updated_elem.text:
        try:
            dt = datetime.fromisoformat(updated_elem.text.replace("Z", "+00:00"))
            fecha_pub = dt.date()
        except (ValueError, TypeError):
            pass

    # Extract expediente from ID or content
    expediente = _extract_expediente(entry_id, titulo)

    # Try to extract CODICE data from content
    content_elem = entry.find(f"{{{atom_ns}}}content")
    organismo = ""
    estado = ""
    tipo_contrato = ""
    importe = None
    lugar = ""
    cpv_code = ""
    fecha_limite = None

    if content_elem is not None:
        # Content may contain embedded CODICE XML
        codice_data = _parse_codice_content(content_elem, nsmap)
        organismo = codice_data.get("organismo", "")
        estado = codice_data.get("estado", "")
        tipo_contrato = codice_data.get("tipo_contrato", "")
        importe = codice_data.get("importe")
        lugar = codice_data.get("lugar", "")
        cpv_code = codice_data.get("cpv_code", "")
        fecha_limite = codice_data.get("fecha_limite")

    return {
        "expediente": expediente,
        "titulo": titulo[:500],
        "organismo": organismo or None,
        "estado": estado or None,
        "tipo_contrato": tipo_contrato or None,
        "descripcion": descripcion[:2000] if descripcion else None,
        "url_licitacion": url or None,
        "fecha_publicacion": fecha_pub,
        "fecha_limite": fecha_limite,
        "importe_estimado": importe,
        "lugar_ejecucion": lugar or None,
        "cpv_code": cpv_code or None,
    }


def _extract_expediente(entry_id: str, titulo: str) -> str:
    """Extract expediente number from entry ID or title."""
    # Try to get from entry ID (often contains the expediente)
    if entry_id:
        # Common patterns: numbers with slashes or dashes
        m = re.search(r'(\d{2,}[/-]\d{2,}(?:[/-]\d+)*)', entry_id)
        if m:
            return m.group(1)
        # If ID is a URL, use last segment
        if "/" in entry_id:
            return entry_id.split("/")[-1][:100]
        return entry_id[:100]
    return titulo[:50]


def _parse_codice_content(content_elem, nsmap: dict) -> dict:
    """Parse CODICE XML embedded in ATOM content element."""
    result = {}

    # Try common CODICE namespaces
    cbc_ns = nsmap.get("cbc", "urn:dgpe:names:draft:codice:schema:xsd:CommonBasicComponents-2")
    cac_ns = nsmap.get("cac", "urn:dgpe:names:draft:codice:schema:xsd:CommonAggregateComponents-2")

    # Search for ContractingParty (organismo)
    for party in content_elem.iter():
        tag = party.tag.split("}")[-1] if "}" in party.tag else party.tag

        if tag == "Name" and not result.get("organismo"):
            parent_tag = ""
            # Walk up to check parent
            if party.text and party.text.strip():
                result["organismo"] = party.text.strip()

        if tag == "ContractTypeCode":
            if party.text:
                tipo_map = {
                    "1": "Obras",
                    "2": "Suministros",
                    "3": "Servicios",
                    "21": "Obras",
                    "31": "Servicios",
                }
                result["tipo_contrato"] = tipo_map.get(party.text.strip(), party.text.strip())

        if tag == "TenderTypeCode" or tag == "ProcedureCode":
            if party.text:
                result["estado"] = party.text.strip()

        if tag == "TaxExclusiveAmount" or tag == "EstimatedOverallContractAmount":
            if party.text:
                try:
                    result["importe"] = float(party.text.strip())
                except ValueError:
                    pass

        if tag == "CountrySubentityCode" or tag == "CityName":
            if party.text and party.text.strip():
                result["lugar"] = party.text.strip()

        if tag == "ItemClassificationCode":
            if party.text and party.text.strip():
                result["cpv_code"] = party.text.strip()

        if tag == "EndDate":
            if party.text:
                try:
                    result["fecha_limite"] = date.fromisoformat(party.text.strip()[:10])
                except ValueError:
                    pass

    return result
