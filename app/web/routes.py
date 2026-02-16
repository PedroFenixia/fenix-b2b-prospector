from __future__ import annotations

"""Server-rendered web routes using Jinja2 + HTMX."""
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import (
    SESSION_COOKIE,
    authenticate_user,
    create_session,
    create_user,
    get_current_user,
    PLAN_LIMITS,
)
from app.db.engine import get_db
from app.schemas.opportunity import OpportunityFilters
from app.schemas.search import SearchFilters
from app.services.company_service import get_company, search_companies
from app.services.ingestion_orchestrator import get_ingestion_status
from app.services.opportunity_service import cross_search, search_judicial, search_subsidies, search_tenders
from app.services.watchlist_service import count_unread_alerts, get_alerts, get_watchlist, is_watched
from app.utils.cnae import get_all_cnae
from app.utils.provinces import get_all_provinces

templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))


def _format_eu(value):
    """Format number with European convention: 1.234.567,89"""
    if value is None:
        return "-"
    try:
        value = float(value)
    except (ValueError, TypeError):
        return str(value)
    if value == int(value):
        formatted = f"{int(value):,}".replace(",", ".")
    else:
        formatted = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return formatted


templates.env.filters["eu"] = _format_eu

web_router = APIRouter()


def _ctx(request: Request, **kwargs) -> dict:
    """Build template context with user info."""
    user = getattr(request.state, "user", None)
    ctx = {"request": request, "user": user}
    if user:
        ctx["plan_limits"] = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
    ctx.update(kwargs)
    return ctx


# --- Auth routes ---

@web_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@web_router.post("/login")
async def login_submit(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    email = form.get("email", "").strip()
    password = form.get("password", "")

    user = await authenticate_user(email, password, db)
    if user:
        token = create_session(user.id, user.email, user.role, user.plan)
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(SESSION_COOKIE, token, httponly=True, max_age=86400)
        return response

    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": "Email o contrasena incorrectos",
        "email": email,
    }, status_code=401)


@web_router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request, error: str = ""):
    return templates.TemplateResponse("register.html", {
        "request": request,
        "error": error,
    })


@web_router.post("/register")
async def register_submit(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    email = form.get("email", "").strip().lower()
    nombre = form.get("nombre", "").strip()
    empresa = form.get("empresa", "").strip() or None
    password = form.get("password", "")
    password2 = form.get("password2", "")

    if not email or not nombre or not password:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Todos los campos son obligatorios",
            "email": email, "nombre": nombre, "empresa": empresa,
        }, status_code=400)

    if password != password2:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Las contrasenas no coinciden",
            "email": email, "nombre": nombre, "empresa": empresa,
        }, status_code=400)

    if len(password) < 6:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "La contrasena debe tener al menos 6 caracteres",
            "email": email, "nombre": nombre, "empresa": empresa,
        }, status_code=400)

    # Check if email exists
    from sqlalchemy import select
    from app.db.models import User
    existing = await db.scalar(select(User).where(User.email == email))
    if existing:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Ya existe una cuenta con ese email",
            "nombre": nombre, "empresa": empresa,
        }, status_code=400)

    user = await create_user(email, nombre, password, db, empresa=empresa)
    token = create_session(user.id, user.email, user.role, user.plan)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(SESSION_COOKIE, token, httponly=True, max_age=86400)
    return response


@web_router.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    return templates.TemplateResponse("pricing.html", {"request": request})


@web_router.get("/logout")
async def logout(request: Request):
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie(SESSION_COOKIE)
    return response


@web_router.get("/account", response_class=HTMLResponse)
async def account_page(request: Request, db: AsyncSession = Depends(get_db)):
    user = get_current_user(request)
    from sqlalchemy import select
    from app.db.models import User
    db_user = await db.get(User, user["user_id"])
    return templates.TemplateResponse("account.html", _ctx(
        request, active_page="account", db_user=db_user,
    ))


# --- Dashboard ---

@web_router.get("/", response_class=HTMLResponse)
async def index(request: Request, db: AsyncSession = Depends(get_db)):
    from app.api.stats import get_stats
    stats = await get_stats(db)
    return templates.TemplateResponse("index.html", _ctx(
        request, stats=stats, active_page="dashboard",
    ))


# --- Search ---

@web_router.get("/search", response_class=HTMLResponse)
async def search_page(request: Request, db: AsyncSession = Depends(get_db)):
    provinces = get_all_provinces()
    cnae_codes = get_all_cnae()
    return templates.TemplateResponse("search.html", _ctx(
        request, provinces=provinces, cnae_codes=cnae_codes, active_page="search",
    ))


@web_router.get("/search/results", response_class=HTMLResponse)
async def search_results(
    request: Request,
    q: str | None = None,
    cif: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    score_min: int | None = None,
    sort_by: str = "fecha_ultima_publicacion",
    sort_order: str = "desc",
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    filters = SearchFilters(
        q=q, cif=cif, provincia=provincia, forma_juridica=forma_juridica,
        cnae_code=cnae_code, estado=estado,
        pub_desde=pub_desde or None, pub_hasta=pub_hasta or None,
        score_min=score_min, sort_by=sort_by, sort_order=sort_order,
        page=page, per_page=per_page,
    )
    result = await search_companies(filters, db)
    return templates.TemplateResponse("partials/company_table.html", _ctx(
        request, companies=result["items"], total=result["total"],
        page=result["page"], pages=result["pages"], per_page=result["per_page"],
        filters=filters,
    ))


@web_router.get("/companies/{company_id}", response_class=HTMLResponse)
async def company_detail(
    request: Request,
    company_id: int,
    db: AsyncSession = Depends(get_db),
):
    company = await get_company(company_id, db)
    if not company:
        return HTMLResponse("<h1>Empresa no encontrada</h1>", status_code=404)
    user = get_current_user(request)
    user_id = user["user_id"] if user else None
    watched = await is_watched(company_id, db, user_id=user_id)
    return templates.TemplateResponse("company_detail.html", _ctx(
        request, company=company, watched=watched, active_page="search",
    ))


# --- Ingestion ---

@web_router.get("/ingestion", response_class=HTMLResponse)
async def ingestion_page(request: Request, db: AsyncSession = Depends(get_db)):
    from sqlalchemy import select
    from app.db.models import IngestionLog

    status = get_ingestion_status()
    recent = await db.scalars(
        select(IngestionLog).order_by(IngestionLog.fecha_borme.desc()).limit(30)
    )
    return templates.TemplateResponse("ingestion.html", _ctx(
        request, status=status, recent_jobs=recent.all(), active_page="ingestion",
    ))


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
    return templates.TemplateResponse("opportunities.html", _ctx(
        request, active_page="opportunities",
    ))


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
        fecha_desde=fecha_desde or None, fecha_hasta=fecha_hasta or None,
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
        fecha_desde=fecha_desde or None, fecha_hasta=fecha_hasta or None,
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
        fecha_desde=fecha_desde or None, fecha_hasta=fecha_hasta or None,
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


# --- Watchlist ---

@web_router.get("/watchlist", response_class=HTMLResponse)
async def watchlist_page(request: Request, db: AsyncSession = Depends(get_db)):
    user = get_current_user(request)
    user_id = user["user_id"] if user else None
    unread = await count_unread_alerts(db, user_id=user_id)
    return templates.TemplateResponse("watchlist.html", _ctx(
        request, unread_count=unread, active_page="watchlist",
    ))


@web_router.get("/watchlist/list", response_class=HTMLResponse)
async def watchlist_list(
    request: Request,
    page: int = 1,
    db: AsyncSession = Depends(get_db),
):
    user = get_current_user(request)
    user_id = user["user_id"] if user else None
    result = await get_watchlist(db, page=page, user_id=user_id)
    return templates.TemplateResponse("partials/watchlist_table.html", {
        "request": request,
        **result,
    })


@web_router.get("/watchlist/alerts", response_class=HTMLResponse)
async def alerts_page(
    request: Request,
    solo_no_leidas: int = 0,
    page: int = 1,
    db: AsyncSession = Depends(get_db),
):
    user = get_current_user(request)
    user_id = user["user_id"] if user else None
    unread = await count_unread_alerts(db, user_id=user_id)
    alerts_result = await get_alerts(db, solo_no_leidas=bool(solo_no_leidas), page=page, user_id=user_id)
    return templates.TemplateResponse("alerts.html", _ctx(
        request, alerts=alerts_result, unread_count=unread,
        solo_no_leidas=bool(solo_no_leidas), active_page="watchlist",
    ))
