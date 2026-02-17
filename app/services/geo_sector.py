"""Mapeos geográficos (CCAA/provincia) y sectoriales (CPV->CNAE) para oportunidades."""
from __future__ import annotations

import re
from typing import Optional

# Provincias por Comunidad Autónoma
CCAA_PROVINCIAS: dict[str, list[str]] = {
    "Andalucía": ["Almería", "Cádiz", "Córdoba", "Granada", "Huelva", "Jaén", "Málaga", "Sevilla"],
    "Aragón": ["Huesca", "Teruel", "Zaragoza"],
    "Asturias": ["Asturias"],
    "Islas Baleares": ["Illes Balears", "Baleares", "Mallorca", "Menorca", "Ibiza"],
    "Canarias": ["Las Palmas", "Santa Cruz de Tenerife", "Tenerife", "Gran Canaria"],
    "Cantabria": ["Cantabria"],
    "Castilla y León": ["Ávila", "Burgos", "León", "Palencia", "Salamanca", "Segovia", "Soria", "Valladolid", "Zamora"],
    "Castilla-La Mancha": ["Albacete", "Ciudad Real", "Cuenca", "Guadalajara", "Toledo"],
    "Cataluña": ["Barcelona", "Girona", "Lleida", "Tarragona"],
    "Comunidad Valenciana": ["Alicante", "Castellón", "Valencia"],
    "Extremadura": ["Badajoz", "Cáceres"],
    "Galicia": ["A Coruña", "Lugo", "Ourense", "Pontevedra"],
    "Comunidad de Madrid": ["Madrid"],
    "Región de Murcia": ["Murcia"],
    "Navarra": ["Navarra"],
    "País Vasco": ["Álava", "Gipuzkoa", "Bizkaia", "Vizcaya", "Guipúzcoa"],
    "La Rioja": ["La Rioja"],
    "Ceuta": ["Ceuta"],
    "Melilla": ["Melilla"],
}

# Invertir: provincia -> CCAA
_PROV_TO_CCAA: dict[str, str] = {}
for ccaa, provs in CCAA_PROVINCIAS.items():
    for p in provs:
        _PROV_TO_CCAA[p.lower()] = ccaa

# Keywords en nombres de organismos que identifican la CCAA
_CCAA_ORG_KEYWORDS: dict[str, list[str]] = {
    "Andalucía": ["junta de andalucía", "andaluz", "andalucia"],
    "Aragón": ["gobierno de aragón", "aragón", "aragon"],
    "Asturias": ["principado de asturias", "asturias"],
    "Islas Baleares": ["govern de les illes balears", "baleares", "balear"],
    "Canarias": ["gobierno de canarias", "canaria"],
    "Cantabria": ["gobierno de cantabria", "cantabria"],
    "Castilla y León": ["junta de castilla y león", "castilla y león", "castilla y leon"],
    "Castilla-La Mancha": ["junta de comunidades de castilla-la mancha", "castilla-la mancha", "castilla la mancha"],
    "Cataluña": ["generalitat de catalunya", "generalitat", "cataluña", "catalunya"],
    "Comunidad Valenciana": ["generalitat valenciana", "comunitat valenciana", "valencia"],
    "Extremadura": ["junta de extremadura", "extremadura"],
    "Galicia": ["xunta de galicia", "galicia", "galega"],
    "Comunidad de Madrid": ["comunidad de madrid"],
    "Región de Murcia": ["región de murcia", "murcia"],
    "Navarra": ["gobierno de navarra", "navarra", "nafarroa"],
    "País Vasco": ["gobierno vasco", "eusko jaurlaritza", "país vasco", "euskadi"],
    "La Rioja": ["gobierno de la rioja", "la rioja"],
    "Ceuta": ["ciudad autónoma de ceuta", "ceuta"],
    "Melilla": ["ciudad autónoma de melilla", "melilla"],
}

# CPV (2 primeros dígitos) -> CNAE principal
_CPV_TO_CNAE: dict[str, tuple[str, str]] = {
    "03": ("01", "Agricultura, ganadería, caza"),
    "09": ("06", "Extracción de petróleo y gas"),
    "14": ("08", "Otras industrias extractivas"),
    "15": ("10", "Industria alimentaria"),
    "18": ("14", "Confección de prendas de vestir"),
    "22": ("17", "Industria del papel"),
    "24": ("20", "Industria química"),
    "30": ("26", "Productos informáticos y electrónicos"),
    "31": ("27", "Material eléctrico"),
    "32": ("26", "Equipos de telecomunicaciones"),
    "33": ("32", "Instrumentos médicos y ópticos"),
    "34": ("29", "Vehículos de motor"),
    "35": ("25", "Productos metálicos"),
    "37": ("32", "Instrumentos musicales y deportivos"),
    "38": ("26", "Instrumentos de medida"),
    "39": ("31", "Muebles"),
    "42": ("28", "Maquinaria"),
    "43": ("28", "Maquinaria industrial"),
    "44": ("25", "Estructuras y productos metálicos"),
    "45": ("41", "Construcción de edificios"),
    "48": ("62", "Programación informática"),
    "50": ("33", "Reparación e instalación de maquinaria"),
    "51": ("33", "Instalación de maquinaria"),
    "55": ("55", "Servicios de alojamiento"),
    "60": ("49", "Transporte terrestre"),
    "63": ("52", "Almacenamiento y transporte"),
    "64": ("53", "Actividades postales"),
    "66": ("64", "Servicios financieros"),
    "70": ("62", "Servicios de TI y consultoría"),
    "71": ("71", "Arquitectura e ingeniería"),
    "72": ("62", "Programación y consultoría informática"),
    "73": ("72", "Investigación y desarrollo"),
    "75": ("84", "Administración pública"),
    "76": ("06", "Servicios de explotación petrolera"),
    "77": ("01", "Servicios agrícolas y forestales"),
    "79": ("69", "Servicios jurídicos y de contabilidad"),
    "80": ("85", "Educación"),
    "85": ("86", "Actividades sanitarias"),
    "90": ("38", "Recogida y tratamiento de residuos"),
    "92": ("90", "Actividades de creación artística"),
    "98": ("96", "Otros servicios personales"),
}


def provincia_to_ccaa(provincia: str) -> Optional[str]:
    """Obtener CCAA a partir de una provincia."""
    if not provincia:
        return None
    return _PROV_TO_CCAA.get(provincia.lower().strip())


def detect_ccaa_from_text(text: str) -> Optional[str]:
    """Detectar CCAA a partir de un texto (organismo, ámbito, etc.)."""
    if not text:
        return None
    text_lower = text.lower()
    for ccaa, keywords in _CCAA_ORG_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                return ccaa
    return None


def detect_provincia_from_text(text: str) -> Optional[str]:
    """Detectar provincia a partir de un texto."""
    if not text:
        return None
    text_lower = text.lower()
    for prov_lower, _ in _PROV_TO_CCAA.items():
        if prov_lower in text_lower:
            # Devolver con capitalización correcta
            for provs in CCAA_PROVINCIAS.values():
                for p in provs:
                    if p.lower() == prov_lower:
                        return p
    return None


def cpv_to_cnae(cpv_code: str) -> Optional[str]:
    """Convertir código CPV a CNAE (primeros 2 dígitos)."""
    if not cpv_code:
        return None
    prefix = cpv_code[:2]
    entry = _CPV_TO_CNAE.get(prefix)
    return entry[0] if entry else None


def cpv_to_sector(cpv_code: str) -> Optional[str]:
    """Obtener descripción del sector a partir de CPV."""
    if not cpv_code:
        return None
    prefix = cpv_code[:2]
    entry = _CPV_TO_CNAE.get(prefix)
    return entry[1] if entry else None
