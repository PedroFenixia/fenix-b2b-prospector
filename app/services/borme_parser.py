from __future__ import annotations

"""Parse BORME PDF files to extract company data.

Uses pdfminer.six for text extraction and regex patterns modeled on
bormeparser's extraction logic.
"""
import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path

from pdfminer.high_level import extract_text

logger = logging.getLogger(__name__)

# -- Act types recognized in BORME Section A --
ACT_TYPES = [
    "Constitución",
    "Nombramientos",
    "Ceses/Dimisiones",
    "Revocaciones",
    "Cambio de domicilio social",
    "Cambio de objeto social",
    "Cambio de denominación social",
    "Ampliación de capital",
    "Reducción de capital",
    "Modificación de estatutos",
    "Disolución",
    "Liquidación",
    "Extinción",
    "Fusión",
    "Escisión",
    "Situación concursal",
    "Depósito de cuentas",
    "Reelecciones",
    "Emisión de obligaciones",
    "Transformación de sociedad",
    "Cancelaciones de oficio de nombramientos",
    "Declaración de unipersonalidad",
    "Pérdida del carácter de unipersonalidad",
    "Ampliación de objeto social",
    "Fe de erratas",
    "Otros conceptos",
]

ACT_PATTERN = re.compile(
    r"(" + "|".join(re.escape(a) for a in ACT_TYPES) + r")\.\s*",
    re.IGNORECASE,
)

# Company entry header: "123 - EMPRESA EJEMPLO SL." or "123.- EMPRESA EJEMPLO SL."
RE_COMPANY_HEADER = re.compile(
    r"^(\d+)\s*[\.\-]+\s*(.+?)(?:\.\s*$|\.$)",
    re.MULTILINE,
)

# Capital: "Capital: 3.000,00 Euros." or "Capital suscrito: 60.000 Euros."
RE_CAPITAL = re.compile(
    r"Capital(?:\s+suscrito)?:\s+([\d\.,]+)\s+(Euros?|€|Pesetas)",
    re.IGNORECASE,
)

# Constitution date: "Comienzo de operaciones: 15.01.25" or "15/01/2025"
RE_FECHA_INICIO = re.compile(
    r"Comienzo\s+de\s+operaciones:\s+(\d{1,2}[./]\d{1,2}[./]\d{2,4})",
    re.IGNORECASE,
)

# Objeto social: "Objeto social: text..."
RE_OBJETO = re.compile(
    r"Objeto\s+social:\s+(.+?)(?=\.\s+Domicilio:|\.\s+Capital:|\.\s+Comienzo|\.\s*$)",
    re.DOTALL | re.IGNORECASE,
)

# Domicilio: "Domicilio: text..."
RE_DOMICILIO = re.compile(
    r"Domicilio:\s+(.+?)(?=\.\s+Capital:|\.\s+Objeto\s+social:|\.\s+Comienzo|\.\s+Datos|\.\s*$)",
    re.DOTALL | re.IGNORECASE,
)

# Officer: "Adm. Unico: NAME" or "Presidente: NAME"
RE_CARGO = re.compile(
    r"(Adm\.\s*(?:Unico|Unica|Solid|Mancom)|Presidente|Vice[Pp]residente|"
    r"Secretario|Consejero|Liquidador|Auditor(?:\s+de\s+cuentas)?|Apoderado|"
    r"Director\s+General|Cons\.Del(?:eg)?)\s*[:\.]?\s*(.+?)(?=;|(?:Adm\.\s)|(?:Presidente)|"
    r"(?:Secretario)|(?:Consejero)|(?:Liquidador)|(?:Auditor)|(?:Apoderado)|"
    r"(?:Director)|(?:Cons\.)|$)",
    re.IGNORECASE,
)


@dataclass
class ParsedOfficer:
    nombre: str
    cargo: str


@dataclass
class ParsedAct:
    tipo: str
    texto: str
    officers: list[ParsedOfficer] = field(default_factory=list)


@dataclass
class ParsedCompany:
    numero: int
    nombre: str
    actos: list[ParsedAct] = field(default_factory=list)
    domicilio: str | None = None
    objeto_social: str | None = None
    capital: float | None = None
    capital_moneda: str = "EUR"
    fecha_inicio: str | None = None


def parse_borme_pdf(pdf_path: Path) -> list[ParsedCompany]:
    """Parse a BORME Section A PDF and extract company data."""
    try:
        text = extract_text(str(pdf_path))
    except Exception as e:
        logger.error(f"Failed to extract text from {pdf_path}: {e}")
        return []

    if not text or len(text.strip()) < 50:
        logger.warning(f"Empty or too short PDF: {pdf_path}")
        return []

    return _parse_text(text)


def _parse_text(text: str) -> list[ParsedCompany]:
    """Parse extracted PDF text into structured company data."""
    companies: list[ParsedCompany] = []

    # Split into company blocks by finding headers like "123 - COMPANY NAME."
    # We find all headers first, then extract text between them
    headers = list(RE_COMPANY_HEADER.finditer(text))

    if not headers:
        logger.warning("No company headers found in text")
        return []

    for i, match in enumerate(headers):
        numero = int(match.group(1))
        nombre = match.group(2).strip()

        # Get the text block for this company (until next header or end)
        start = match.end()
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block = text[start:end].strip()

        company = ParsedCompany(numero=numero, nombre=nombre)
        _parse_company_block(company, block)
        companies.append(company)

    logger.info(f"Parsed {len(companies)} companies from text")
    return companies


def _parse_company_block(company: ParsedCompany, block: str):
    """Parse a single company's text block to extract acts and data."""
    # Find all act types in the block
    act_matches = list(ACT_PATTERN.finditer(block))

    if not act_matches:
        # No recognized acts - store the whole block as a generic act
        company.actos.append(ParsedAct(tipo="Otros conceptos", texto=block))
    else:
        for i, match in enumerate(act_matches):
            tipo = match.group(1)
            start = match.end()
            end = act_matches[i + 1].start() if i + 1 < len(act_matches) else len(block)
            act_text = block[start:end].strip()

            act = ParsedAct(tipo=tipo, texto=act_text)

            # Extract officers from Nombramientos/Ceses
            if tipo in ("Nombramientos", "Ceses/Dimisiones", "Reelecciones", "Revocaciones"):
                act.officers = _extract_officers(act_text)

            company.actos.append(act)

    # Extract data from Constitución act if present
    constitucion_text = None
    for act in company.actos:
        if act.tipo == "Constitución":
            constitucion_text = act.texto
            break

    # Also try the full block for these fields
    search_text = constitucion_text or block

    # Capital
    cap_match = RE_CAPITAL.search(search_text)
    if cap_match:
        raw_amount = cap_match.group(1)
        moneda = cap_match.group(2)
        try:
            amount = float(raw_amount.replace(".", "").replace(",", "."))
            company.capital = amount
            company.capital_moneda = "PTS" if "peseta" in moneda.lower() else "EUR"
        except ValueError:
            pass

    # Domicilio
    dom_match = RE_DOMICILIO.search(search_text)
    if dom_match:
        company.domicilio = " ".join(dom_match.group(1).split())

    # Objeto social
    obj_match = RE_OBJETO.search(search_text)
    if obj_match:
        company.objeto_social = " ".join(obj_match.group(1).split())

    # Fecha de inicio
    fecha_match = RE_FECHA_INICIO.search(search_text)
    if fecha_match:
        company.fecha_inicio = fecha_match.group(1)


def _extract_officers(text: str) -> list[ParsedOfficer]:
    """Extract officer names and roles from appointment/resignation text."""
    officers = []
    for match in RE_CARGO.finditer(text):
        cargo = match.group(1).strip().rstrip(":")
        nombre_raw = match.group(2).strip().rstrip(";.").strip()
        # Clean up multiple names separated by semicolons
        names = [n.strip() for n in nombre_raw.split(";") if n.strip()]
        for name in names:
            if len(name) > 2:
                officers.append(ParsedOfficer(nombre=name, cargo=cargo))
    return officers


def parsed_to_json(companies: list[ParsedCompany]) -> str:
    """Serialize parsed companies to JSON for storage."""
    data = []
    for c in companies:
        data.append({
            "numero": c.numero,
            "nombre": c.nombre,
            "domicilio": c.domicilio,
            "objeto_social": c.objeto_social,
            "capital": c.capital,
            "capital_moneda": c.capital_moneda,
            "fecha_inicio": c.fecha_inicio,
            "actos": [
                {
                    "tipo": a.tipo,
                    "officers": [{"nombre": o.nombre, "cargo": o.cargo} for o in a.officers],
                }
                for a in c.actos
            ],
        })
    return json.dumps(data, ensure_ascii=False)
