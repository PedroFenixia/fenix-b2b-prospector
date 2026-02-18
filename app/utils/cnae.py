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


def guess_cnae(objeto_social: str) -> str | None:
    """Best-effort CNAE code from objeto_social text. Returns division code or None."""
    if not objeto_social:
        return None

    # 1. Try to extract explicit CNAE codes from text (e.g. "6202 / ACTIVIDADES DE...")
    explicit = re.findall(r"\b(\d{4})\b", objeto_social)
    if explicit:
        # Use the first 4-digit code's first 2 digits as division
        division = explicit[0][:2]
        # Verify it's a valid CNAE division (01-99)
        codes = _load()
        valid_divisions = {item.get("code", "")[:2] for item in codes if item.get("code")}
        if division in valid_divisions:
            return division

    # 2. Keyword matching (normalize accents for comparison)
    text = unidecode(objeto_social).lower()
    best_code = None
    best_count = 0
    for code, keywords in CNAE_KEYWORDS.items():
        count = sum(1 for kw in keywords if unidecode(kw) in text)
        if count > best_count:
            best_count = count
            best_code = code
    return best_code if best_count > 0 else None
