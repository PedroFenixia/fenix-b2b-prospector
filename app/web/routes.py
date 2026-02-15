from __future__ import annotations

"""Server-rendered web routes using Jinja2 + HTMX."""
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.schemas.opportunity import OpportunityFilters
from app.schemas.search import SearchFilters
from app.services.company_service import get_company, search_companies
from app.services.ingestion_orchestrator import get_ingestion_status
from app.services.opportunity_service import search_subsidies, search_tenders
from app.utils.cnae import get_all_cnae
from app.utils.provinces import get_all_provinces

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

web_router = APIRouter()


@web_router.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    """Dashboard page."""
    # Quick stats
    from app.api.stats import get_stats
    stats = await get_stats(db)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stats": stats,
        "active_page": "dashboard",
    })


@web_router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, db: AsyncSession = Depends(get_db)):
    """Search page."""
    provinces = get_all_provinces()
    cnae_codes = get_all_cnae()
    return templates.TemplateResponse("search.html", {
        "request": request,
        "provinces": provinces,
        "cnae_codes": cnae_codes,
        "active_page": "search",
    })


@web_router.get("/search/results", response_class=HTMLResponse)
async def search_results(
    request: Request,
    q: str | None = None,
    cif: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    sort_by: str = "fecha_ultima_publicacion",
    sort_order: str = "desc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    """HTMX partial: search results table."""
    filters = SearchFilters(
        q=q, cif=cif, provincia=provincia, forma_juridica=forma_juridica,
        cnae_code=cnae_code, estado=estado, sort_by=sort_by,
        sort_order=sort_order, page=page, per_page=per_page,
    )
    result = await search_companies(filters, db)
    return templates.TemplateResponse("partials/company_table.html", {
        "request": request,
        "companies": result["items"],
        "total": result["total"],
        "page": result["page"],
        "pages": result["pages"],
        "per_page": result["per_page"],
        "filters": filters,
    })


@web_router.get("/companies/{company_id}", response_class=HTMLResponse)
async def company_detail(
    request: Request,
    company_id: int,
    db: AsyncSession = Depends(get_db),
):
    """Company detail page."""
    company = await get_company(company_id, db)
    if not company:
        return HTMLResponse("<h1>Empresa no encontrada</h1>", status_code=404)
    return templates.TemplateResponse("company_detail.html", {
        "request": request,
        "company": company,
        "active_page": "search",
    })


@web_router.get("/ingestion", response_class=HTMLResponse)
async def ingestion_page(request: Request, db: AsyncSession = Depends(get_db)):
    """Ingestion control panel."""
    from sqlalchemy import select
    from app.db.models import IngestionLog

    status = get_ingestion_status()
    recent = await db.scalars(
        select(IngestionLog).order_by(IngestionLog.fecha_borme.desc()).limit(30)
    )
    return templates.TemplateResponse("ingestion.html", {
        "request": request,
        "status": status,
        "recent_jobs": recent.all(),
        "active_page": "ingestion",
    })


@web_router.get("/ingestion/status-partial", response_class=HTMLResponse)
async def ingestion_status_partial(request: Request, db: AsyncSession = Depends(get_db)):
    """HTMX partial: ingestion status badge."""
    status = get_ingestion_status()
    from sqlalchemy import select
    from app.db.models import IngestionLog
    recent = await db.scalars(
        select(IngestionLog).order_by(IngestionLog.fecha_borme.desc()).limit(10)
    )
    return templates.TemplateResponse("partials/ingestion_status.html", {
        "request": request,
        "status": status,
        "recent_jobs": recent.all(),
    })


@web_router.get("/opportunities", response_class=HTMLResponse)
async def opportunities_page(request: Request):
    """Opportunities page (subsidies + tenders)."""
    return templates.TemplateResponse("opportunities.html", {
        "request": request,
        "active_page": "opportunities",
    })


@web_router.get("/opportunities/subsidies-results", response_class=HTMLResponse)
async def subsidies_results(
    request: Request,
    q: str | None = None,
    organismo: str | None = None,
    sector: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    """HTMX partial: subsidies results table."""
    filters = OpportunityFilters(
        q=q, organismo=organismo, sector=sector,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        page=page, per_page=per_page,
    )
    result = await search_subsidies(filters, db)
    return templates.TemplateResponse("partials/subsidies_table.html", {
        "request": request,
        "subsidies": result["items"],
        "total": result["total"],
        "page": result["page"],
        "pages": result["pages"],
        "per_page": result["per_page"],
    })


@web_router.get("/opportunities/tenders-results", response_class=HTMLResponse)
async def tenders_results(
    request: Request,
    q: str | None = None,
    organismo: str | None = None,
    tipo_contrato: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    """HTMX partial: tenders results table."""
    filters = OpportunityFilters(
        q=q, organismo=organismo, tipo_contrato=tipo_contrato,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        page=page, per_page=per_page,
    )
    result = await search_tenders(filters, db)
    return templates.TemplateResponse("partials/tenders_table.html", {
        "request": request,
        "tenders": result["items"],
        "total": result["total"],
        "page": result["page"],
        "pages": result["pages"],
        "per_page": result["per_page"],
    })
