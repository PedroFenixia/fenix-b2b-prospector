from __future__ import annotations

"""Company search and CRUD service."""
import logging
import math
import re
from datetime import datetime, time

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.db.models import Act, Company, Officer
from app.schemas.search import SearchFilters

# CIF pattern: letter + 7 digits + control (digit or letter)
_CIF_RE = re.compile(r'^[ABCDEFGHJKLMNPQRSUVW]\d{7}[0-9A-J]$', re.IGNORECASE)

def _is_pg() -> bool:
    return settings.database_url.startswith("postgresql")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Typesense search path
# ---------------------------------------------------------------------------

def _date_to_ts(d) -> int:
    """date → unix timestamp."""
    if d is None:
        return 0
    from datetime import date as _date
    if isinstance(d, str):
        d = _date.fromisoformat(d)
    return int(datetime.combine(d, time.min).timestamp())


def _build_typesense_filter(filters: SearchFilters) -> str:
    """Convierte SearchFilters en filter_by de Typesense."""
    parts: list[str] = []

    if filters.cif:
        cif_clean = filters.cif.strip().upper()
        parts.append(f"cif:={cif_clean}")

    if filters.provincia:
        parts.append(f"provincia:={filters.provincia}")

    if filters.forma_juridica:
        parts.append(f"forma_juridica:={filters.forma_juridica}")

    if filters.cnae_code:
        parts.append(f"cnae_code:={filters.cnae_code}")

    if filters.estado:
        parts.append(f"estado:={filters.estado}")

    if filters.score_min is not None:
        parts.append(f"score_solvencia:>={filters.score_min}")

    if filters.capital_min is not None:
        parts.append(f"capital_social:>={filters.capital_min}")

    if filters.capital_max is not None:
        parts.append(f"capital_social:<={filters.capital_max}")

    if filters.pub_desde:
        ts = _date_to_ts(filters.pub_desde)
        parts.append(f"fecha_ultima_publicacion:>={ts}")

    if filters.pub_hasta:
        ts = _date_to_ts(filters.pub_hasta)
        parts.append(f"fecha_ultima_publicacion:<={ts}")

    if filters.fecha_desde:
        ts = _date_to_ts(filters.fecha_desde)
        parts.append(f"fecha_constitucion:>={ts}")

    if filters.fecha_hasta:
        ts = _date_to_ts(filters.fecha_hasta)
        parts.append(f"fecha_constitucion:<={ts}")

    return " && ".join(parts)


def _build_typesense_sort(filters: SearchFilters) -> str:
    """Convierte sort_by/sort_order en sort_by de Typesense."""
    ts_sort_map = {
        "nombre": "nombre",
        "fecha_constitucion": "fecha_constitucion",
        "fecha_ultima_publicacion": "fecha_ultima_publicacion",
        "capital_social": "capital_social",
        "provincia": "provincia",
        "score_solvencia": "score_solvencia",
    }
    field = ts_sort_map.get(filters.sort_by, "fecha_ultima_publicacion")
    order = "asc" if filters.sort_order == "asc" else "desc"
    return f"{field}:{order}"


async def _search_via_typesense(filters: SearchFilters, db: AsyncSession) -> dict | None:
    """Intenta buscar via Typesense. Retorna None si falla (fallback a DB)."""
    try:
        from app.services.typesense_service import search_typesense

        q = filters.q or "*"
        filter_by = _build_typesense_filter(filters)
        sort_by = _build_typesense_sort(filters)

        ts_result = await search_typesense(
            q=q,
            filter_by=filter_by,
            sort_by=sort_by,
            page=filters.page,
            per_page=filters.per_page,
        )

        total = ts_result.get("found", 0)
        hits = ts_result.get("hits", [])

        if not hits:
            return {
                "items": [],
                "total": total,
                "page": filters.page,
                "pages": math.ceil(total / filters.per_page) if total > 0 else 1,
                "per_page": filters.per_page,
            }

        # Cargar Company objects desde SQLite por los IDs de la pagina
        hit_ids = [int(h["document"]["id"]) for h in hits]
        result = await db.scalars(
            select(Company).where(Company.id.in_(hit_ids))
        )
        companies_by_id = {c.id: c for c in result.all()}

        # Mantener el orden de Typesense
        items = [companies_by_id[cid] for cid in hit_ids if cid in companies_by_id]

        return {
            "items": items,
            "total": total,
            "page": filters.page,
            "pages": math.ceil(total / filters.per_page) if total > 0 else 1,
            "per_page": filters.per_page,
        }

    except Exception:
        logger.warning("Typesense no disponible, fallback a DB", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Database search (PostgreSQL tsvector + LIKE fallback)
# ---------------------------------------------------------------------------

async def _search_via_db(filters: SearchFilters, db: AsyncSession) -> dict:
    """Busqueda directa en DB (PostgreSQL FTS + LIKE fallback)."""
    conditions = []

    if filters.q:
        q_stripped = filters.q.strip()
        q_upper = q_stripped.upper()

        # Direct CIF lookup (instant via index)
        if _CIF_RE.match(q_upper):
            conditions.append(Company.cif == q_upper)
        elif len(q_upper) <= 3:
            conditions.append(Company.nombre_normalizado.like(f"{q_upper}%"))
        else:
            try:
                if _is_pg():
                    from app.services.fts_service import build_pg_tsquery
                    tsquery_expr = build_pg_tsquery(q_stripped)
                    conditions.append(
                        text(
                            "search_vector @@ to_tsquery(:fts_config, :fts_q)"
                        ).bindparams(fts_config=settings.pg_fts_config, fts_q=tsquery_expr)
                    )
                else:
                    from app.services.fts_service import build_fts_match
                    fts_expr = build_fts_match(q_stripped)
                    conditions.append(
                        text(
                            "companies.id IN (SELECT rowid FROM companies_fts "
                            "WHERE companies_fts MATCH :fts_q)"
                        ).bindparams(fts_q=fts_expr)
                    )
            except Exception:
                logger.debug("FTS not available, falling back to LIKE", exc_info=True)
                conditions.append(
                    (Company.nombre_normalizado.like(f"%{q_upper}%"))
                    | (Company.objeto_social.ilike(f"%{q_stripped}%"))
                    | (Company.cif.ilike(f"%{q_stripped}%"))
                )

    if filters.cif:
        cif_clean = filters.cif.strip().upper()
        if len(cif_clean) >= 9:
            conditions.append(Company.cif == cif_clean)
        else:
            conditions.append(Company.cif.like(f"{cif_clean}%"))

    if filters.provincia:
        conditions.append(Company.provincia == filters.provincia)
    if filters.forma_juridica:
        conditions.append(Company.forma_juridica == filters.forma_juridica)
    if filters.cnae_code:
        conditions.append(Company.cnae_code.startswith(filters.cnae_code))
    if filters.estado:
        conditions.append(Company.estado == filters.estado)
    if filters.fecha_desde:
        conditions.append(Company.fecha_constitucion >= filters.fecha_desde)
    if filters.fecha_hasta:
        conditions.append(Company.fecha_constitucion <= filters.fecha_hasta)
    if filters.pub_desde:
        conditions.append(Company.fecha_ultima_publicacion >= filters.pub_desde)
    if filters.pub_hasta:
        conditions.append(Company.fecha_ultima_publicacion <= filters.pub_hasta)
    if filters.capital_min is not None:
        conditions.append(Company.capital_social >= filters.capital_min)
    if filters.capital_max is not None:
        conditions.append(Company.capital_social <= filters.capital_max)
    if filters.score_min is not None:
        conditions.append(Company.score_solvencia >= filters.score_min)

    has_join = bool(filters.tipo_acto)

    count_query = select(func.count(func.distinct(Company.id)))
    if has_join:
        count_query = count_query.join(Act).where(Act.tipo_acto == filters.tipo_acto)
    for cond in conditions:
        count_query = count_query.where(cond)
    total = await db.scalar(count_query) or 0

    query = select(Company)
    if has_join:
        query = query.join(Act).where(Act.tipo_acto == filters.tipo_acto).distinct()
    for cond in conditions:
        query = query.where(cond)

    sort_columns = {
        "nombre": Company.nombre,
        "fecha_constitucion": Company.fecha_constitucion,
        "fecha_ultima_publicacion": Company.fecha_ultima_publicacion,
        "capital_social": Company.capital_social,
        "provincia": Company.provincia,
        "score_solvencia": Company.score_solvencia,
    }
    sort_col = sort_columns.get(filters.sort_by, Company.fecha_ultima_publicacion)
    if filters.sort_order == "asc":
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    query = query.offset(filters.offset).limit(filters.per_page)
    result = await db.scalars(query)
    items = result.all()

    return {
        "items": items,
        "total": total,
        "page": filters.page,
        "pages": math.ceil(total / filters.per_page) if total > 0 else 1,
        "per_page": filters.per_page,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def search_companies(filters: SearchFilters, db: AsyncSession) -> dict:
    """Search companies with filters. Returns {items, total, page, pages, per_page}.

    Tries Typesense first (fast). Falls back to DB if Typesense is unavailable.
    The tipo_acto filter (join with Acts) is only supported in the DB path.
    """
    use_typesense = bool(settings.typesense_url) and not filters.tipo_acto

    if use_typesense:
        result = await _search_via_typesense(filters, db)
        if result is not None:
            return result

    return await _search_via_db(filters, db)


async def get_company(company_id: int, db: AsyncSession) -> Company | None:
    """Get a single company with its acts and officers."""
    result = await db.execute(
        select(Company)
        .options(selectinload(Company.acts), selectinload(Company.officers))
        .where(Company.id == company_id)
    )
    return result.scalar_one_or_none()


async def get_company_acts(company_id: int, db: AsyncSession) -> list[Act]:
    result = await db.scalars(
        select(Act)
        .where(Act.company_id == company_id)
        .order_by(Act.fecha_publicacion.desc())
    )
    return list(result.all())


async def get_company_officers(company_id: int, db: AsyncSession) -> list[Officer]:
    result = await db.scalars(
        select(Officer)
        .where(Officer.company_id == company_id)
        .order_by(Officer.fecha_publicacion.desc())
    )
    return list(result.all())


async def update_company_cif(company_id: int, cif: str | None, db: AsyncSession) -> Company | None:
    """Update a company's CIF (manual enrichment)."""
    result = await db.execute(select(Company).where(Company.id == company_id))
    company = result.scalar_one_or_none()
    if not company:
        return None
    company.cif = cif.strip().upper() if cif else None
    await db.commit()
    await db.refresh(company)
    return company
