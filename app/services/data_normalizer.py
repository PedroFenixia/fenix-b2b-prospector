from __future__ import annotations

"""Normalize parsed BORME data before storage."""
import logging
import re
from datetime import date

from app.services.borme_parser import ParsedCompany
from app.utils.cnae import guess_cnae
from app.utils.provinces import normalize_province
from app.utils.text_clean import (
    clean_capital,
    extract_forma_juridica,
    extract_provincia_from_domicilio,
    normalize_name,
)

logger = logging.getLogger(__name__)

# Pesetas to EUR conversion (fixed rate since 2002-01-01)
PTS_TO_EUR = 1 / 166.386


def normalize_company(
    parsed: ParsedCompany,
    borme_provincia: str,
    fecha_publicacion: date,
) -> dict:
    """
    Normalize a parsed company into a dict ready for DB upsert.
    Returns a dict with fields matching the Company model.
    """
    nombre = parsed.nombre.strip().rstrip(".")
    nombre_normalizado = normalize_name(nombre)
    forma_juridica = extract_forma_juridica(nombre)

    # Province: try from domicilio first, fall back to BORME section header
    provincia = None
    if parsed.domicilio:
        raw_prov = extract_provincia_from_domicilio(parsed.domicilio)
        if raw_prov:
            provincia = normalize_province(raw_prov)
    if not provincia:
        provincia = normalize_province(borme_provincia)

    # Localidad: try to extract city from domicilio before province parenthetical
    localidad = None
    if parsed.domicilio:
        match = re.search(r"[,\s]+([A-ZÁÉÍÓÚÑ][a-záéíóúñ\s]+)\s*\(", parsed.domicilio)
        if match:
            localidad = match.group(1).strip()

    # Capital: convert pesetas to euros if needed
    capital = parsed.capital
    if capital and parsed.capital_moneda == "PTS":
        capital = round(capital * PTS_TO_EUR, 2)

    # CNAE: best-effort from objeto_social
    cnae_code = guess_cnae(parsed.objeto_social) if parsed.objeto_social else None

    # Fecha constitución from "Comienzo de operaciones"
    fecha_constitucion = _parse_date(parsed.fecha_inicio) if parsed.fecha_inicio else None

    # Estado: infer from act types
    estado = "activa"
    for act in parsed.actos:
        if act.tipo == "Disolución":
            estado = "disuelta"
        elif act.tipo == "Liquidación":
            estado = "en_liquidacion"
        elif act.tipo == "Extinción":
            estado = "extinguida"

    return {
        "nombre": nombre,
        "nombre_normalizado": nombre_normalizado,
        "forma_juridica": forma_juridica,
        "domicilio": parsed.domicilio,
        "provincia": provincia or borme_provincia,
        "localidad": localidad,
        "objeto_social": parsed.objeto_social,
        "cnae_code": cnae_code,
        "capital_social": capital,
        "fecha_constitucion": fecha_constitucion,
        "fecha_primera_publicacion": fecha_publicacion,
        "fecha_ultima_publicacion": fecha_publicacion,
        "estado": estado,
    }


def _parse_date(raw: str) -> date | None:
    """Parse date strings like '15.01.25', '15/01/2025', '15.01.2025'."""
    if not raw:
        return None

    raw = raw.strip().replace("/", ".")
    parts = raw.split(".")
    if len(parts) != 3:
        return None

    try:
        day = int(parts[0])
        month = int(parts[1])
        year = int(parts[2])

        if year < 100:
            year += 2000 if year < 50 else 1900

        return date(year, month, day)
    except (ValueError, IndexError):
        return None
