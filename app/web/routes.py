from __future__ import annotations

"""Server-rendered web routes using Jinja2 + HTMX."""
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    SESSION_COOKIE,
    check_credentials,
    create_session,
    destroy_session,
)
from app.db.engine import get_db
from app.schemas.opportunity import OpportunityFilters
from app.schemas.search import SearchFilters
from app.services.company_service import get_company, search_companies
from app.services.ingestion_orchestrator import get_ingestion_status
from app.services.opportunity_service import cross_search, search_judicial, search_subsidies, search_tenders
from app.utils.cnae import get_all_cnae
from app.utils.provinces import get_all_provinces

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

web_router = APIRouter()


# --- Auth routes ---

@web_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@web_router.post("/login")
async def login_submit(request: Request):
    form = await request.form()
    username = form.get("username", "")
    password = form.get("password", "")

    if check_credentials(username, password):
        token = create_session()
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(SESSION_COOKIE, token, httponly=True, max_age=86400)
        return response

    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": "Usuario o contrasena incorrectos",
    }, status_code=401)


@web_router.get("/logout")
async def logout(request: Request):
    token = request.cookies.get(SESSION_COOKIE)
    destroy_session(token)
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


# --- Dashboard ---

@web_router.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    from app.api.stats import get_stats
    stats = await get_stats(db)
    return templates.TemplateResponse("index.html", {
        "request": request,
        "stats": stats,
        "active_page": "dashboard",
    })


# --- Search ---

@web_router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, db: AsyncSession = Depends(get_db)):
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
    company = await get_company(company_id, db)
    if not company:
        return HTMLResponse("<h1>Empresa no encontrada</h1>", status_code=404)
    return templates.TemplateResponse("company_detail.html", {
        "request": request,
        "company": company,
        "active_page": "search",
    })


# --- Ingestion ---

@web_router.get("/ingestion", response_class=HTMLResponse)
async def ingestion_page(request: Request, db: AsyncSession = Depends(get_db)):
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


# --- Opportunities ---

@web_router.get("/opportunities", response_class=HTMLResponse)
async def opportunities_page(request: Request):
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


@web_router.get("/opportunities/judicial-results", response_class=HTMLResponse)
async def judicial_results(
    request: Request,
    q: str | None = None,
    tipo: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = OpportunityFilters(
        q=q, tipo_contrato=tipo,
        fecha_desde=fecha_desde, fecha_hasta=fecha_hasta,
        page=page, per_page=per_page,
    )
    result = await search_judicial(filters, db)
    return templates.TemplateResponse("partials/judicial_table.html", {
        "request": request,
        "notices": result["items"],
        "total": result["total"],
        "page": result["page"],
        "pages": result["pages"],
        "per_page": result["per_page"],
    })


@web_router.get("/opportunities/cross-search", response_class=HTMLResponse)
async def cross_search_results(
    request: Request,
    cif: str | None = None,
    nombre: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    results = await cross_search(cif, nombre, db)
    return templates.TemplateResponse("partials/cross_search_results.html", {
        "request": request,
        "subsidies": results["subsidies"],
        "tenders": results["tenders"],
        "judicial": results["judicial"],
    })
