from __future__ import annotations

"""Service for searching subsidies, tenders and judicial notices."""
import logging
import math
from datetime import date

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.models import Company, JudicialNotice, Subsidy, Tender
from app.schemas.opportunity import ConciliacionFilters, OpportunityFilters

logger = logging.getLogger(__name__)


async def search_subsidies(filters: OpportunityFilters, db: AsyncSession, include_archived: bool = False) -> dict:
    """Search subsidies with filters. Excludes archived by default."""
    query = select(Subsidy)

    if not include_archived:
        query = query.where(Subsidy.archivada == False)

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


async def search_tenders(filters: OpportunityFilters, db: AsyncSession, include_archived: bool = False) -> dict:
    """Search tenders with filters. Excludes archived by default."""
    query = select(Tender)

    if not include_archived:
        query = query.where(Tender.archivada == False)

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


async def find_opportunities_by_cnae(cnae_code: str, db: AsyncSession, limit: int = 10) -> dict:
    """Find subsidies and tenders matching a CNAE code."""
    if not cnae_code:
        return {"subsidies": [], "tenders": []}

    pattern = f"%{cnae_code}%"

    subsidies = (await db.scalars(
        select(Subsidy)
        .where(Subsidy.cnae_codes.ilike(pattern), Subsidy.archivada == False)
        .order_by(Subsidy.fecha_publicacion.desc())
        .limit(limit)
    )).all()

    tenders = (await db.scalars(
        select(Tender)
        .where(Tender.cnae_codes.ilike(pattern), Tender.archivada == False)
        .order_by(Tender.fecha_publicacion.desc())
        .limit(limit)
    )).all()

    return {"subsidies": subsidies, "tenders": tenders}


async def archive_expired(db: AsyncSession) -> dict:
    """Archive subsidies and tenders whose fecha_limite has passed."""
    today = date.today()

    sub_result = await db.execute(
        update(Subsidy)
        .where(Subsidy.fecha_limite < today, Subsidy.archivada == False)
        .values(archivada=True)
    )
    sub_count = sub_result.rowcount

    tender_result = await db.execute(
        update(Tender)
        .where(Tender.fecha_limite < today, Tender.archivada == False)
        .values(archivada=True)
    )
    tender_count = tender_result.rowcount

    await db.commit()

    if sub_count or tender_count:
        logger.info(f"[Archive] Archived {sub_count} subsidies, {tender_count} tenders (deadline passed)")

    return {"subsidies_archived": sub_count, "tenders_archived": tender_count}


async def search_conciliacion(filters: ConciliacionFilters, db: AsyncSession) -> dict:
    """Find subsidies/tenders that match active companies by CNAE + province."""
    results: list[dict] = []

    # Helper: build JOIN condition for CNAE array matching + province + active
    def _join_cond(opp_table):
        return (
            (Company.cnae_code == func.any_(
                func.string_to_array(func.replace(opp_table.cnae_codes, " ", ""), ",")
            ))
            & (Company.provincia == opp_table.provincia)
            & (Company.estado == "activa")
        )

    # --- Subsidies ---
    if filters.tipo in (None, "subsidies"):
        sub_q = (
            select(Subsidy, func.count(Company.id).label("company_count"))
            .join(Company, _join_cond(Subsidy))
            .where(
                Subsidy.archivada == False,
                Subsidy.cnae_codes.isnot(None),
                Subsidy.cnae_codes != "",
                Subsidy.provincia.isnot(None),
                Subsidy.provincia != "",
            )
            .group_by(Subsidy.id)
        )
        if filters.provincia:
            sub_q = sub_q.where(Subsidy.provincia == filters.provincia)
        if filters.cnae_code:
            sub_q = sub_q.where(Subsidy.cnae_codes.ilike(f"%{filters.cnae_code}%"))
        if filters.q:
            sub_q = sub_q.where(Subsidy.titulo.ilike(f"%{filters.q}%"))

        for row in (await db.execute(sub_q)).all():
            results.append({"type": "subsidy", "opportunity": row[0], "company_count": row[1]})

    # --- Tenders ---
    if filters.tipo in (None, "tenders"):
        tend_q = (
            select(Tender, func.count(Company.id).label("company_count"))
            .join(Company, _join_cond(Tender))
            .where(
                Tender.archivada == False,
                Tender.cnae_codes.isnot(None),
                Tender.cnae_codes != "",
                Tender.provincia.isnot(None),
                Tender.provincia != "",
            )
            .group_by(Tender.id)
        )
        if filters.provincia:
            tend_q = tend_q.where(Tender.provincia == filters.provincia)
        if filters.cnae_code:
            tend_q = tend_q.where(Tender.cnae_codes.ilike(f"%{filters.cnae_code}%"))
        if filters.q:
            tend_q = tend_q.where(Tender.titulo.ilike(f"%{filters.q}%"))

        for row in (await db.execute(tend_q)).all():
            results.append({"type": "tender", "opportunity": row[0], "company_count": row[1]})

    # Sort combined results
    if filters.sort_by == "fecha_publicacion":
        results.sort(key=lambda r: r["opportunity"].fecha_publicacion, reverse=(filters.sort_order == "desc"))
    else:
        results.sort(key=lambda r: (r["company_count"], r["opportunity"].fecha_publicacion), reverse=True)

    # Paginate
    total = len(results)
    pages = max(1, math.ceil(total / filters.per_page))
    items = results[filters.offset:filters.offset + filters.per_page]

    return {"items": items, "total": total, "page": filters.page, "pages": pages}


async def get_conciliacion_companies(opp_type: str, opp_id: int, db: AsyncSession, limit: int = 50) -> list:
    """Get active companies matching a specific opportunity's CNAE + province."""
    if opp_type == "subsidy":
        opp = await db.get(Subsidy, opp_id)
    elif opp_type == "tender":
        opp = await db.get(Tender, opp_id)
    else:
        return []

    if not opp or not opp.cnae_codes or not opp.provincia:
        return []

    q = (
        select(Company)
        .where(
            Company.cnae_code == func.any_(
                func.string_to_array(func.replace(opp.cnae_codes, " ", ""), ",")
            ),
            Company.provincia == opp.provincia,
            Company.estado == "activa",
        )
        .order_by(Company.nombre.asc())
        .limit(limit)
    )
    return (await db.scalars(q)).all()
