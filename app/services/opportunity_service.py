from __future__ import annotations

"""Service for searching subsidies and tenders."""
import math

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import JudicialNotice, Subsidy, Tender
from app.schemas.opportunity import OpportunityFilters


async def search_subsidies(filters: OpportunityFilters, db: AsyncSession) -> dict:
    """Search subsidies with filters."""
    query = select(Subsidy)

    if filters.q:
        pattern = f"%{filters.q}%"
        query = query.where(
            (Subsidy.titulo.ilike(pattern))
            | (Subsidy.descripcion.ilike(pattern))
            | (Subsidy.organismo.ilike(pattern))
        )

    if filters.organismo:
        query = query.where(Subsidy.organismo.ilike(f"%{filters.organismo}%"))

    if filters.sector:
        query = query.where(Subsidy.sector.ilike(f"%{filters.sector}%"))

    if filters.fecha_desde:
        query = query.where(Subsidy.fecha_publicacion >= filters.fecha_desde)

    if filters.fecha_hasta:
        query = query.where(Subsidy.fecha_publicacion <= filters.fecha_hasta)

    if filters.importe_min is not None:
        query = query.where(Subsidy.importe >= filters.importe_min)

    if filters.importe_max is not None:
        query = query.where(Subsidy.importe <= filters.importe_max)

    # Count
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query) or 0

    # Sort
    sort_cols = {
        "fecha_publicacion": Subsidy.fecha_publicacion,
        "titulo": Subsidy.titulo,
        "importe": Subsidy.importe,
        "organismo": Subsidy.organismo,
    }
    sort_col = sort_cols.get(filters.sort_by, Subsidy.fecha_publicacion)
    if filters.sort_order == "asc":
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    # Paginate
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


async def search_tenders(filters: OpportunityFilters, db: AsyncSession) -> dict:
    """Search tenders with filters."""
    query = select(Tender)

    if filters.q:
        pattern = f"%{filters.q}%"
        query = query.where(
            (Tender.titulo.ilike(pattern))
            | (Tender.descripcion.ilike(pattern))
            | (Tender.organismo.ilike(pattern))
            | (Tender.expediente.ilike(pattern))
        )

    if filters.organismo:
        query = query.where(Tender.organismo.ilike(f"%{filters.organismo}%"))

    if filters.tipo_contrato:
        query = query.where(Tender.tipo_contrato == filters.tipo_contrato)

    if filters.fecha_desde:
        query = query.where(Tender.fecha_publicacion >= filters.fecha_desde)

    if filters.fecha_hasta:
        query = query.where(Tender.fecha_publicacion <= filters.fecha_hasta)

    if filters.importe_min is not None:
        query = query.where(Tender.importe_estimado >= filters.importe_min)

    if filters.importe_max is not None:
        query = query.where(Tender.importe_estimado <= filters.importe_max)

    # Count
    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query) or 0

    # Sort
    sort_cols = {
        "fecha_publicacion": Tender.fecha_publicacion,
        "titulo": Tender.titulo,
        "importe_estimado": Tender.importe_estimado,
        "organismo": Tender.organismo,
    }
    sort_col = sort_cols.get(filters.sort_by, Tender.fecha_publicacion)
    if filters.sort_order == "asc":
        query = query.order_by(sort_col.asc())
    else:
        query = query.order_by(sort_col.desc())

    # Paginate
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


async def upsert_subsidies(subsidies: list[dict], db: AsyncSession) -> int:
    """Insert or ignore subsidies (dedup by boe_id)."""
    count = 0
    for s in subsidies:
        existing = await db.scalar(
            select(Subsidy).where(Subsidy.boe_id == s["boe_id"])
        )
        if existing:
            continue
        db.add(Subsidy(**s))
        count += 1
    await db.commit()
    return count


async def upsert_tenders(tenders: list[dict], db: AsyncSession) -> int:
    """Insert or ignore tenders (dedup by expediente)."""
    count = 0
    for t in tenders:
        existing = await db.scalar(
            select(Tender).where(Tender.expediente == t["expediente"])
        )
        if existing:
            continue
        db.add(Tender(**t))
        count += 1
    await db.commit()
    return count


async def search_judicial(filters: OpportunityFilters, db: AsyncSession) -> dict:
    """Search judicial notices with filters."""
    query = select(JudicialNotice)

    if filters.q:
        pattern = f"%{filters.q}%"
        query = query.where(
            (JudicialNotice.titulo.ilike(pattern))
            | (JudicialNotice.descripcion.ilike(pattern))
            | (JudicialNotice.deudor.ilike(pattern))
            | (JudicialNotice.juzgado.ilike(pattern))
        )

    if filters.organismo:
        query = query.where(JudicialNotice.juzgado.ilike(f"%{filters.organismo}%"))

    if filters.tipo_contrato:
        query = query.where(JudicialNotice.tipo == filters.tipo_contrato)

    if filters.fecha_desde:
        query = query.where(JudicialNotice.fecha_publicacion >= filters.fecha_desde)

    if filters.fecha_hasta:
        query = query.where(JudicialNotice.fecha_publicacion <= filters.fecha_hasta)

    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query) or 0

    sort_cols = {
        "fecha_publicacion": JudicialNotice.fecha_publicacion,
        "titulo": JudicialNotice.titulo,
        "tipo": JudicialNotice.tipo,
    }
    sort_col = sort_cols.get(filters.sort_by, JudicialNotice.fecha_publicacion)
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


async def cross_search(cif: str | None, nombre: str | None, db: AsyncSession) -> dict:
    """Search across subsidies, tenders and judicial by CIF or company name."""
    results: dict = {"subsidies": [], "tenders": [], "judicial": []}

    terms: list[str] = []
    if cif and cif.strip():
        terms.append(cif.strip())
    if nombre and nombre.strip():
        terms.append(nombre.strip())

    if not terms:
        return results

    # Search subsidies
    for term in terms:
        pattern = f"%{term}%"
        q = select(Subsidy).where(
            (Subsidy.titulo.ilike(pattern))
            | (Subsidy.descripcion.ilike(pattern))
            | (Subsidy.beneficiarios.ilike(pattern))
        ).order_by(Subsidy.fecha_publicacion.desc()).limit(50)
        res = await db.scalars(q)
        for item in res.all():
            if item not in results["subsidies"]:
                results["subsidies"].append(item)

    # Search tenders
    for term in terms:
        pattern = f"%{term}%"
        q = select(Tender).where(
            (Tender.titulo.ilike(pattern))
            | (Tender.descripcion.ilike(pattern))
            | (Tender.expediente.ilike(pattern))
        ).order_by(Tender.fecha_publicacion.desc()).limit(50)
        res = await db.scalars(q)
        for item in res.all():
            if item not in results["tenders"]:
                results["tenders"].append(item)

    # Search judicial
    for term in terms:
        pattern = f"%{term}%"
        q = select(JudicialNotice).where(
            (JudicialNotice.titulo.ilike(pattern))
            | (JudicialNotice.descripcion.ilike(pattern))
            | (JudicialNotice.deudor.ilike(pattern))
        ).order_by(JudicialNotice.fecha_publicacion.desc()).limit(50)
        res = await db.scalars(q)
        for item in res.all():
            if item not in results["judicial"]:
                results["judicial"].append(item)

    return results


async def upsert_judicial(notices: list[dict], db: AsyncSession) -> int:
    """Insert or ignore judicial notices (dedup by boe_id)."""
    count = 0
    for n in notices:
        existing = await db.scalar(
            select(JudicialNotice).where(JudicialNotice.boe_id == n["boe_id"])
        )
        if existing:
            continue
        db.add(JudicialNotice(**n))
        count += 1
    await db.commit()
    return count
