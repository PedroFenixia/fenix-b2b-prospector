from __future__ import annotations

"""Company search and CRUD service."""
import logging
import math

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.models import Act, Company, Officer
from app.schemas.search import SearchFilters

logger = logging.getLogger(__name__)


async def search_companies(filters: SearchFilters, db: AsyncSession) -> dict:
    """Search companies with filters. Returns {items, total, page, pages, per_page}."""
    query = select(Company)

    # Free-text search on nombre_normalizado and objeto_social
    if filters.q:
        pattern = f"%{filters.q.upper()}%"
        query = query.where(
            (Company.nombre_normalizado.like(pattern))
            | (Company.objeto_social.ilike(f"%{filters.q}%"))
        )

    if filters.provincia:
        query = query.where(Company.provincia == filters.provincia)

    if filters.forma_juridica:
        query = query.where(Company.forma_juridica == filters.forma_juridica)

    if filters.cnae_code:
        query = query.where(Company.cnae_code.startswith(filters.cnae_code))

    if filters.estado:
        query = query.where(Company.estado == filters.estado)

    if filters.fecha_desde:
        query = query.where(Company.fecha_constitucion >= filters.fecha_desde)

    if filters.fecha_hasta:
        query = query.where(Company.fecha_constitucion <= filters.fecha_hasta)

    if filters.pub_desde:
        query = query.where(Company.fecha_ultima_publicacion >= filters.pub_desde)

    if filters.pub_hasta:
        query = query.where(Company.fecha_ultima_publicacion <= filters.pub_hasta)

    if filters.capital_min is not None:
        query = query.where(Company.capital_social >= filters.capital_min)

    if filters.capital_max is not None:
        query = query.where(Company.capital_social <= filters.capital_max)

    if filters.tipo_acto:
        query = query.join(Act).where(Act.tipo_acto == filters.tipo_acto)

    # Count total
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query) or 0

    # Sorting
    sort_columns = {
        "nombre": Company.nombre,
        "fecha_constitucion": Company.fecha_constitucion,
        "fecha_ultima_publicacion": Company.fecha_ultima_publicacion,
        "capital_social": Company.capital_social,
        "provincia": Company.provincia,
    }
    sort_col = sort_columns.get(filters.sort_by, Company.fecha_ultima_publicacion)
    if filters.sort_order == "asc":
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    # Pagination
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
