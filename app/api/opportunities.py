from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.schemas.opportunity import (
    OpportunityFilters,
    PaginatedSubsidies,
    PaginatedTenders,
    SubsidyOut,
    TenderOut,
)
from app.services.opportunity_service import search_judicial, search_subsidies, search_tenders

router = APIRouter()


@router.get("/subsidies", response_model=PaginatedSubsidies)
async def list_subsidies(
    q: str | None = None,
    organismo: str | None = None,
    sector: str | None = None,
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    importe_min: float | None = None,
    importe_max: float | None = None,
    sort_by: str = "fecha_publicacion",
    sort_order: str = "desc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = OpportunityFilters(
        q=q, organismo=organismo, sector=sector,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        importe_min=importe_min, importe_max=importe_max,
        sort_by=sort_by, sort_order=sort_order,
        page=page, per_page=per_page,
    )
    result = await search_subsidies(filters, db)
    return PaginatedSubsidies(
        items=[SubsidyOut.model_validate(s) for s in result["items"]],
        total=result["total"],
        page=result["page"],
        pages=result["pages"],
        per_page=result["per_page"],
    )


@router.get("/tenders", response_model=PaginatedTenders)
async def list_tenders(
    q: str | None = None,
    organismo: str | None = None,
    tipo_contrato: str | None = None,
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    importe_min: float | None = None,
    importe_max: float | None = None,
    sort_by: str = "fecha_publicacion",
    sort_order: str = "desc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = OpportunityFilters(
        q=q, organismo=organismo, tipo_contrato=tipo_contrato,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        importe_min=importe_min, importe_max=importe_max,
        sort_by=sort_by, sort_order=sort_order,
        page=page, per_page=per_page,
    )
    result = await search_tenders(filters, db)
    return PaginatedTenders(
        items=[TenderOut.model_validate(t) for t in result["items"]],
        total=result["total"],
        page=result["page"],
        pages=result["pages"],
        per_page=result["per_page"],
    )


@router.get("/judicial")
async def list_judicial(
    q: str | None = None,
    tipo: str | None = None,
    fecha_desde: date | None = None,
    fecha_hasta: date | None = None,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = OpportunityFilters(
        q=q, tipo_contrato=tipo,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        page=page, per_page=per_page,
    )
    return await search_judicial(filters, db)


@router.post("/fetch-subsidies")
async def trigger_fetch_subsidies(
    fecha: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Trigger fetching subsidies from BOE for a given date."""
    from app.services.boe_subsidies_fetcher import fetch_boe_subsidies
    from app.services.opportunity_service import upsert_subsidies

    target_date = fecha or date.today()
    raw = await fetch_boe_subsidies(target_date)
    count = await upsert_subsidies(raw, db)
    return {"date": target_date.isoformat(), "fetched": len(raw), "new": count}


@router.post("/fetch-tenders")
async def trigger_fetch_tenders(
    db: AsyncSession = Depends(get_db),
):
    """Trigger fetching recent tenders from PLACSP."""
    from app.services.placsp_fetcher import fetch_recent_tenders
    from app.services.opportunity_service import upsert_tenders

    raw = await fetch_recent_tenders(max_entries=100)
    count = await upsert_tenders(raw, db)
    return {"fetched": len(raw), "new": count}


@router.post("/fetch-judicial")
async def trigger_fetch_judicial(
    fecha: date | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Trigger fetching judicial notices from BOE."""
    from app.services.boe_judicial_fetcher import fetch_boe_judicial
    from app.services.opportunity_service import upsert_judicial

    target_date = fecha or date.today()
    raw = await fetch_boe_judicial(target_date)
    count = await upsert_judicial(raw, db)
    return {"date": target_date.isoformat(), "fetched": len(raw), "new": count}
