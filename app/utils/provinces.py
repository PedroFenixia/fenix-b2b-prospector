from __future__ import annotations

import json
from pathlib import Path

_DATA_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "provinces.json"
_PROVINCES: list[dict] | None = None


def _load() -> list[dict]:
    global _PROVINCES
    if _PROVINCES is None:
        with open(_DATA_FILE, encoding="utf-8") as f:
            _PROVINCES = json.load(f)
    return _PROVINCES


def get_all_provinces() -> list[dict]:
    return _load()


def get_province_names() -> list[str]:
    return [p["nombre"] for p in _load()]


_BORME_PROVINCE_MAP: dict[str, str] | None = None


def normalize_province(raw: str) -> str | None:
    """Match a raw province string from BORME to a canonical name."""
    global _BORME_PROVINCE_MAP
    if _BORME_PROVINCE_MAP is None:
        _BORME_PROVINCE_MAP = {}
        for p in _load():
            name = p["nombre"]
            _BORME_PROVINCE_MAP[name.upper()] = name
        # Common BORME variations
        _BORME_PROVINCE_MAP.update({
            "ALAVA": "Álava",
            "ARABA": "Álava",
            "BIZKAIA": "Vizcaya",
            "GIPUZKOA": "Guipúzcoa",
            "GUIPUZCOA": "Guipúzcoa",
            "ILLES BALEARS": "Baleares",
            "ISLAS BALEARES": "Baleares",
            "GIRONA": "Girona",
            "GERONA": "Girona",
            "LLEIDA": "Lleida",
            "LERIDA": "Lleida",
            "OURENSE": "Ourense",
            "ORENSE": "Ourense",
            "A CORUÑA": "A Coruña",
            "LA CORUÑA": "A Coruña",
            "SANTA CRUZ DE TENERIFE": "Santa Cruz de Tenerife",
            "S.C. TENERIFE": "Santa Cruz de Tenerife",
            "SC TENERIFE": "Santa Cruz de Tenerife",
            "LAS PALMAS": "Las Palmas",
        })

    return _BORME_PROVINCE_MAP.get(raw.strip().upper())
