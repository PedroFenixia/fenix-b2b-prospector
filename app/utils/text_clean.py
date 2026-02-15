from __future__ import annotations

import re

from unidecode import unidecode


def normalize_name(name: str) -> str:
    """Uppercase, strip accents, collapse whitespace."""
    text = unidecode(name).upper().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def extract_forma_juridica(nombre: str) -> str | None:
    """Extract legal form from company name suffix."""
    patterns = [
        (r"\bS\.?L\.?U\.?\b", "SLU"),
        (r"\bS\.?L\.?L\.?\b", "SLL"),
        (r"\bS\.?L\.?\b", "SL"),
        (r"\bS\.?A\.?U\.?\b", "SAU"),
        (r"\bS\.?A\.?\b", "SA"),
        (r"\bS\.?C\.?O{0,2}P\.?\b", "SCOOP"),
        (r"\bS\.?C\.?\b", "SC"),
        (r"\bSOCIEDAD LIMITADA\b", "SL"),
        (r"\bSOCIEDAD ANONIMA\b", "SA"),
        (r"\bSOCIEDAD COOPERATIVA\b", "SCOOP"),
        (r"\bCOMUNIDAD DE BIENES\b", "CB"),
        (r"\bC\.?B\.?\b", "CB"),
    ]
    upper = nombre.upper()
    for pattern, forma in patterns:
        if re.search(pattern, upper):
            return forma
    return None


def extract_provincia_from_domicilio(domicilio: str) -> str | None:
    """Try to extract province from a domicilio string like '... MADRID (MADRID)'."""
    match = re.search(r"\(([A-ZÁÉÍÓÚÑ\s]+)\)\s*\.?\s*$", domicilio.upper())
    if match:
        return match.group(1).strip()
    return None


def clean_capital(raw: str) -> float | None:
    """Parse capital string like '3.000,00' into float 3000.00."""
    try:
        cleaned = raw.replace(".", "").replace(",", ".")
        return float(cleaned)
    except (ValueError, AttributeError):
        return None
