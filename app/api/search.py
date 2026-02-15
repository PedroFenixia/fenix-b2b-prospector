from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.schemas.company import CompanyOut
from app.schemas.search import PaginatedResponse, SearchFilters
from app.services.company_service import search_companies

router = APIRouter()


@router.get("", response_model=PaginatedResponse)
async def search(
    q: str | None = None,
    cif: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    tipo_acto: str | None = None,
    estado: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    capital_min: float | None = None,
    capital_max: float | None = None,
    sort_by: str = "fecha_ultima_publicacion",
    sort_order: str = "desc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = SearchFilters(
        q=q,
        cif=cif,
        provincia=provincia,
        forma_juridica=forma_juridica,
        cnae_code=cnae_code,
        tipo_acto=tipo_acto,
        estado=estado,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        pub_desde=pub_desde,
        pub_hasta=pub_hasta,
        capital_min=capital_min,
        capital_max=capital_max,
        sort_by=sort_by,
        sort_order=sort_order,
        page=page,
        per_page=per_page,
    )
    result = await search_companies(filters, db)
    return PaginatedResponse(
        items=[CompanyOut.model_validate(c) for c in result["items"]],
        total=result["total"],
        page=result["page"],
        pages=result["pages"],
        per_page=result["per_page"],
    )
