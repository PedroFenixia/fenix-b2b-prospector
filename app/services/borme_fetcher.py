from __future__ import annotations

"""Fetch BORME sumario from BOE open data API."""
import logging
from dataclasses import dataclass, field
from datetime import date

import httpx
from lxml import etree

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class BormePdfEntry:
    id: str
    titulo: str
    url_pdf: str
    provincia: str


@dataclass
class BormeSumario:
    fecha: date
    pdfs: list[BormePdfEntry] = field(default_factory=list)


async def fetch_sumario(fecha: date) -> BormeSumario | None:
    """
    Fetch BORME sumario for a given date.
    Returns None if no BORME was published (weekends/holidays).
    Only extracts Section A (Actos inscritos) which contains company data.
    """
    url = f"{settings.boe_api_base}/borme/sumario/{fecha.strftime('%Y%m%d')}"
    logger.info(f"Fetching BORME sumario: {url}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.get(url, headers={"Accept": "application/xml"})

    if resp.status_code == 404:
        logger.info(f"No BORME published for {fecha} (404)")
        return None

    if resp.status_code != 200:
        logger.error(f"BOE API error {resp.status_code} for {fecha}")
        raise Exception(f"BOE API returned {resp.status_code}")

    return _parse_sumario_xml(fecha, resp.content)


def _parse_sumario_xml(fecha: date, xml_content: bytes) -> BormeSumario:
    """Parse the BORME sumario XML to extract PDF URLs for Section A.

    Real XML structure:
    response > data > sumario > diario > seccion[@codigo='A'] > item
    Each item has: <identificador>, <titulo> (province name), <url_pdf>
    """
    root = etree.fromstring(xml_content)
    sumario = BormeSumario(fecha=fecha)

    # Find Section A (Actos inscritos)
    for seccion in root.iter("seccion"):
        codigo = seccion.get("codigo", "")
        if codigo != "A":
            continue

        # Items are directly inside <seccion>, no intermediate <departamento>
        for item in seccion.findall("item"):
            # <identificador>BORME-A-2025-28-02</identificador>
            id_elem = item.find("identificador")
            item_id = id_elem.text.strip() if id_elem is not None and id_elem.text else ""

            # <titulo>ALBACETE</titulo> — this is the province name
            titulo_elem = item.find("titulo")
            provincia = titulo_elem.text.strip() if titulo_elem is not None and titulo_elem.text else "Desconocida"

            # <url_pdf>https://www.boe.es/borme/dias/...</url_pdf>
            url_pdf_elem = item.find("url_pdf")
            url_pdf = ""
            if url_pdf_elem is not None and url_pdf_elem.text:
                url_pdf = url_pdf_elem.text.strip()
                if url_pdf and not url_pdf.startswith("http"):
                    url_pdf = f"https://www.boe.es{url_pdf}"

            if url_pdf and item_id:
                sumario.pdfs.append(BormePdfEntry(
                    id=item_id,
                    titulo=provincia,
                    url_pdf=url_pdf,
                    provincia=provincia,
                ))

    logger.info(f"Found {len(sumario.pdfs)} PDFs for {fecha}")
    return sumario
