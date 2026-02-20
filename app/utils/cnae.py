from __future__ import annotations

import json
import re
from pathlib import Path

from unidecode import unidecode

_DATA_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "cnae_codes.json"
_CODES: list[dict] | None = None

# Keyword-to-CNAE mapping for best-effort classification from objeto_social
CNAE_KEYWORDS: dict[str, list[str]] = {
    "01": ["agricultura", "ganadería", "cultivo", "explotación agrícola"],
    "10": ["alimentación", "productos alimenticios", "elaboración de alimentos"],
    "41": ["construcción de edificios", "promoción inmobiliaria", "construcción"],
    "43": ["reformas", "instalaciones eléctricas", "fontanería", "pintura"],
    "45": ["venta de vehículos", "taller mecánico", "reparación de vehículos"],
    "46": ["comercio al por mayor", "distribución", "importación", "exportación"],
    "47": ["comercio al por menor", "venta al público", "tienda"],
    "49": ["transporte de mercancías", "transporte de viajeros", "mudanzas"],
    "55": ["hotel", "alojamiento", "hostal", "apartamento turístico"],
    "56": ["restaurante", "bar", "cafetería", "catering", "comidas"],
    "62": ["desarrollo de software", "programación", "aplicaciones informáticas", "consultoría informática", "tecnología de la información"],
    "63": ["procesamiento de datos", "hosting", "portales web"],
    "64": ["servicios financieros", "intermediación financiera"],
    "65": ["seguros", "reaseguros", "correduría de seguros"],
    "68": ["inmobiliaria", "compraventa de inmuebles", "alquiler de inmuebles", "gestión inmobiliaria", "arrendamiento"],
    "69": ["asesoría fiscal", "asesoría contable", "asesoría jurídica", "abogados", "contabilidad", "auditoría"],
    "70": ["consultoría de gestión", "consultoría empresarial", "asesoramiento empresarial"],
    "71": ["arquitectura", "ingeniería", "servicios técnicos"],
    "72": ["investigación", "desarrollo experimental", "I+D"],
    "73": ["publicidad", "marketing", "estudios de mercado", "relaciones públicas"],
    "74": ["diseño", "fotografía", "traducción"],
    "77": ["alquiler de maquinaria", "alquiler de vehículos", "leasing"],
    "79": ["agencia de viajes", "turismo", "operador turístico"],
    "82": ["servicios administrativos", "centro de llamadas", "call center"],
    "85": ["educación", "formación", "enseñanza", "academia"],
    "86": ["clínica", "consulta médica", "odontología", "fisioterapia", "medicina"],
    "93": ["gimnasio", "deporte", "actividades deportivas", "fitness"],
    "96": ["peluquería", "estética", "lavandería", "servicios personales"],
}


def _load() -> list[dict]:
    global _CODES
    if _CODES is None:
        with open(_DATA_FILE, encoding="utf-8") as f:
            _CODES = json.load(f)
    return _CODES


def get_all_cnae() -> list[dict]:
    return _load()


def get_cnae_description(code: str) -> str | None:
    """Get CNAE description for a given code."""
    if not code:
        return None
    for item in _load():
        if item.get("code") == code:
            return item.get("description") or item.get("name")
    return None


def _extract_cnae_after_keyword(text: str, valid_divisions: set[str]) -> str | None:
    """Extract CNAE division code from explicit 'CNAE' mentions in text."""
    for m in re.finditer(r"CNAE", text, re.IGNORECASE):
        rest = text[m.end():]
        # Skip optional year reference: " 2009)", "-2009", etc.
        year_m = re.match(r"[\s:\-]*20\d{2}\s*\)?", rest)
        if year_m:
            rest = rest[year_m.end():]
        # Skip optional "actividad principal:" / "de la actividad principal"
        act_m = re.match(
            r"[\s:\-]*(?:actividad\s+principal|de\s+la\s+actividad\s+princ(?:ipal)?)\s*:?\s*",
            rest, re.IGNORECASE,
        )
        if act_m:
            rest = rest[act_m.end():]
        # Skip separators (space, colon, hyphen)
        sep_m = re.match(r"[\s:\-]+", rest)
        if sep_m:
            rest = rest[sep_m.end():]
        # Extract code: "59.15", "5610", "43.2", "68,10", "6421", "9002Domicilio"
        code_m = re.match(r"(\d{2})[.,]?(\d{1,2})?(?!\d)", rest)
        if code_m:
            division = code_m.group(1)
            if division in valid_divisions:
                return division
    return None


def guess_cnae(objeto_social: str) -> str | None:
    """Best-effort CNAE code from objeto_social text. Returns division code or None."""
    if not objeto_social:
        return None

    # Valid CNAE-2009 divisions (01-99, excluding unused ranges)
    valid_divisions = {f"{i:02d}" for i in range(1, 100)}

    # 1. Look for explicit "CNAE" keyword followed by a code
    found = _extract_cnae_after_keyword(objeto_social, valid_divisions)
    if found:
        return found

    # 2. Fallback: any bare 4-digit code that isn't a year (19xx/20xx)
    for m in re.finditer(r"\b(\d{4})\b", objeto_social):
        code4 = m.group(1)
        if code4[:2] in ("19", "20"):
            continue  # skip years
        division = code4[:2]
        if division in valid_divisions:
            return division

    # 3. Keyword matching (normalize accents for comparison)
    text = unidecode(objeto_social).lower()
    best_code = None
    best_count = 0
    for code, keywords in CNAE_KEYWORDS.items():
        count = sum(1 for kw in keywords if unidecode(kw) in text)
        if count > best_count:
            best_count = count
            best_code = code
    return best_code if best_count > 0 else None
