"""FTS5 full-text search with Spanish business synonym expansion."""
from __future__ import annotations

import logging
from unidecode import unidecode

logger = logging.getLogger(__name__)

# Spanish business synonym groups - searching any term finds all related terms
SYNONYM_GROUPS = [
    {"construccion", "obras", "edificacion", "edificar", "constructora", "reformas"},
    {"inmobiliaria", "inmuebles", "viviendas", "pisos", "promocion inmobiliaria", "fincas"},
    {"tecnologia", "informatica", "software", "digital", "sistemas", "tic", "tech"},
    {"consultoria", "asesoria", "consulting", "asesores", "consultores"},
    {"transporte", "logistica", "distribucion", "mensajeria", "mudanzas"},
    {"alimentacion", "alimentos", "comida", "alimentaria", "agroalimentaria"},
    {"hosteleria", "restauracion", "restaurante", "bar", "catering", "hotel"},
    {"comercio", "venta", "tienda", "comercial", "retail", "compraventa"},
    {"energia", "electrica", "electricidad", "renovable", "solar", "fotovoltaica"},
    {"salud", "sanidad", "clinica", "medico", "sanitario", "hospital", "farmacia"},
    {"educacion", "formacion", "ensenanza", "academia", "escuela", "colegio"},
    {"seguridad", "vigilancia", "proteccion", "alarmas", "custodia"},
    {"limpieza", "mantenimiento", "higiene", "servicios integrales"},
    {"marketing", "publicidad", "comunicacion", "medios", "agencia"},
    {"agricultura", "ganaderia", "agraria", "agropecuaria", "cultivos"},
    {"textil", "confeccion", "moda", "ropa", "prendas", "calzado"},
    {"metalurgia", "metalica", "acero", "hierro", "fundicion", "siderurgia"},
    {"quimica", "quimicos", "farmaceutica", "laboratorio", "biotecnologia"},
    {"madera", "carpinteria", "muebles", "mobiliario", "ebanisteria"},
    {"papel", "imprenta", "editorial", "artes graficas", "impresion"},
    {"plastico", "envases", "embalaje", "packaging", "envasado"},
    {"ceramica", "azulejos", "baldosas", "porcelana", "vidrio"},
    {"vehiculo", "automovil", "coche", "automocion", "taller", "concesionario"},
    {"seguros", "aseguradora", "corredurias", "polizas", "reaseguro"},
    {"financiera", "inversion", "capital", "credito", "prestamos", "banca"},
    {"abogado", "juridico", "legal", "derecho", "bufete", "procurador"},
    {"ingenieria", "proyecto", "estudio tecnico", "oficina tecnica"},
    {"telecomunicacion", "telecom", "fibra", "redes", "comunicaciones"},
    {"turismo", "viajes", "agencia viajes", "ocio", "aventura"},
    {"deporte", "gimnasio", "fitness", "deportivo", "club"},
    {"peluqueria", "estetica", "belleza", "spa", "bienestar"},
    {"dental", "dentista", "odontologia", "ortodoncia", "clinica dental"},
    {"optica", "optometria", "gafas", "lentes", "vision"},
    {"veterinaria", "veterinario", "animal", "mascotas", "clinica veterinaria"},
    {"reciclaje", "residuos", "medio ambiente", "medioambiental", "gestion residuos"},
    {"fontaneria", "fontanero", "instalaciones", "calefaccion", "climatizacion"},
    {"pintura", "pintores", "decoracion", "interiorismo", "diseno"},
]

# Build lookup: word -> set of synonyms
_SYNONYM_MAP: dict[str, set[str]] = {}
for group in SYNONYM_GROUPS:
    normalized = {unidecode(w).upper() for w in group}
    for word in normalized:
        _SYNONYM_MAP[word] = normalized


def expand_query(query: str) -> list[str]:
    """Expand a search query with synonyms. Returns list of terms to search."""
    q_normalized = unidecode(query.strip()).upper()
    words = q_normalized.split()

    expanded = {q_normalized}  # Always include original

    for word in words:
        if word in _SYNONYM_MAP:
            expanded.update(_SYNONYM_MAP[word])

    return list(expanded)


def build_fts_match(query: str) -> str:
    """Build an FTS5 MATCH expression with synonym expansion.

    Example: "construccion madrid" →
      '(construccion OR obras OR edificacion OR constructora OR reformas) madrid'
    """
    q_normalized = unidecode(query.strip()).upper()
    words = q_normalized.split()

    parts = []
    for word in words:
        if len(word) < 2:
            continue
        if word in _SYNONYM_MAP:
            synonyms = _SYNONYM_MAP[word]
            # FTS5 OR expression
            or_expr = " OR ".join(f'"{s}"' for s in synonyms)
            parts.append(f"({or_expr})")
        else:
            # Use prefix match for individual words
            parts.append(f'"{word}"*')

    if not parts:
        return f'"{q_normalized}"*'

    return " ".join(parts)


# --- FTS5 table management ---

CREATE_FTS_TABLE = """
CREATE VIRTUAL TABLE IF NOT EXISTS companies_fts USING fts5(
    nombre_normalizado,
    objeto_social,
    content='companies',
    content_rowid='id',
    tokenize='unicode61 remove_diacritics 2'
);
"""

POPULATE_FTS = """
INSERT INTO companies_fts(rowid, nombre_normalizado, objeto_social)
SELECT id, COALESCE(nombre_normalizado, ''), COALESCE(objeto_social, '')
FROM companies;
"""

# Triggers to keep FTS in sync
CREATE_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS companies_fts_insert AFTER INSERT ON companies BEGIN
    INSERT INTO companies_fts(rowid, nombre_normalizado, objeto_social)
    VALUES (new.id, COALESCE(new.nombre_normalizado, ''), COALESCE(new.objeto_social, ''));
END;

CREATE TRIGGER IF NOT EXISTS companies_fts_update AFTER UPDATE ON companies BEGIN
    INSERT INTO companies_fts(companies_fts, rowid, nombre_normalizado, objeto_social)
    VALUES ('delete', old.id, COALESCE(old.nombre_normalizado, ''), COALESCE(old.objeto_social, ''));
    INSERT INTO companies_fts(rowid, nombre_normalizado, objeto_social)
    VALUES (new.id, COALESCE(new.nombre_normalizado, ''), COALESCE(new.objeto_social, ''));
END;

CREATE TRIGGER IF NOT EXISTS companies_fts_delete AFTER DELETE ON companies BEGIN
    INSERT INTO companies_fts(companies_fts, rowid, nombre_normalizado, objeto_social)
    VALUES ('delete', old.id, COALESCE(old.nombre_normalizado, ''), COALESCE(old.objeto_social, ''));
END;
"""
