"""PostgreSQL full-text search with Spanish business synonym expansion."""
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
    """Build an FTS5 MATCH expression with synonym expansion for SQLite.

    Example: "construccion madrid" ->
      '(construccion OR obras OR edificacion OR constructora OR reformas) AND madrid*'
    """
    q_normalized = unidecode(query.strip()).upper()
    words = q_normalized.split()

    parts = []
    for word in words:
        if len(word) < 2:
            continue
        if word in _SYNONYM_MAP:
            synonyms = _SYNONYM_MAP[word]
            or_terms = " OR ".join(f'"{s.lower()}"' for s in synonyms)
            parts.append(f"({or_terms})")
        else:
            parts.append(f"{word.lower()}*")

    if not parts:
        return f"{q_normalized.lower()}*"

    return " AND ".join(parts)


def build_pg_tsquery(query: str) -> str:
    """Build a PostgreSQL tsquery expression with synonym expansion.

    Example: "construccion madrid" ->
      "(construccion | obras | edificacion | constructora | reformas) & madrid:*"
    """
    q_normalized = unidecode(query.strip()).upper()
    words = q_normalized.split()

    parts = []
    for word in words:
        if len(word) < 2:
            continue
        word_lower = word.lower()
        if word in _SYNONYM_MAP:
            synonyms = _SYNONYM_MAP[word]
            # PostgreSQL OR syntax with |
            or_expr = " | ".join(s.lower().replace(" ", " <-> ") for s in synonyms)
            parts.append(f"({or_expr})")
        else:
            # Prefix match for individual words
            parts.append(f"{word_lower}:*")

    if not parts:
        return f"{q_normalized.lower()}:*"

    # AND all parts together
    return " & ".join(parts)


# --- PostgreSQL FTS setup (run once at startup) ---

CREATE_SEARCH_VECTOR_COLUMN = """
ALTER TABLE companies ADD COLUMN IF NOT EXISTS search_vector tsvector;
"""

POPULATE_SEARCH_VECTOR = """
UPDATE companies SET search_vector =
    setweight(to_tsvector('fenix_spanish', COALESCE(nombre_normalizado, '')), 'A') ||
    setweight(to_tsvector('fenix_spanish', COALESCE(objeto_social, '')), 'B')
WHERE search_vector IS NULL;
"""

CREATE_GIN_INDEX = """
CREATE INDEX IF NOT EXISTS idx_companies_search_vector
ON companies USING gin(search_vector);
"""

CREATE_SEARCH_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION companies_search_vector_update() RETURNS trigger AS $$
BEGIN
    NEW.search_vector :=
        setweight(to_tsvector('fenix_spanish', COALESCE(NEW.nombre_normalizado, '')), 'A') ||
        setweight(to_tsvector('fenix_spanish', COALESCE(NEW.objeto_social, '')), 'B');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

DROP_SEARCH_TRIGGER = """
DROP TRIGGER IF EXISTS trig_companies_search_vector ON companies;
"""

CREATE_SEARCH_TRIGGER = """
CREATE TRIGGER trig_companies_search_vector
    BEFORE INSERT OR UPDATE OF nombre_normalizado, objeto_social ON companies
    FOR EACH ROW
    EXECUTE FUNCTION companies_search_vector_update();
"""
