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
from app.config import settings
from app.db.engine import get_db
from app.schemas.opportunity import OpportunityFilters
from app.schemas.search import SearchFilters
from app.services.company_service import get_company, search_companies
from app.services.ingestion_orchestrator import get_ingestion_status
from app.services.opportunity_service import cross_search, search_judicial, search_subsidies, search_tenders
from app.services.watchlist_service import count_unread_alerts, get_act_type_watches, get_alerts, get_watchlist, is_watched
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

# Rate limiting: track failed login attempts per IP
import time as _time
_login_attempts: dict[str, list[float]] = {}  # ip -> [timestamps]
_LOGIN_MAX_ATTEMPTS = 5
_LOGIN_WINDOW = 300  # 5 minutes


def _check_login_rate(ip: str) -> bool:
    """Return True if login is allowed, False if rate-limited."""
    now = _time.time()
    attempts = _login_attempts.get(ip, [])
    # Clean old attempts
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW]
    _login_attempts[ip] = attempts
    return len(attempts) < _LOGIN_MAX_ATTEMPTS


def _record_login_attempt(ip: str):
    now = _time.time()
    _login_attempts.setdefault(ip, []).append(now)


@web_router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
    })


@web_router.post("/login")
async def login_submit(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    email = form.get("email", "").strip().lower()
    password = form.get("password", "")

    # Rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if not _check_login_rate(client_ip):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Demasiados intentos. Espera 5 minutos.",
            "email": email,
        }, status_code=429)

    user = await authenticate_user(email, password, db)
    if user:
        # If email not verified and SMTP is configured, redirect to verification
        if not user.email_verified and settings.smtp_host:
            from app.services.email_service import generate_code, send_verification_email
            from datetime import datetime as _dt
            code = generate_code()
            user.verification_code = code
            user.verification_code_at = _dt.utcnow()
            await db.commit()
            await send_verification_email(email, code, user.nombre)
            return templates.TemplateResponse("verify_email.html", {
                "request": request, "email": email,
            })

        token = create_session(user.id, user.email, user.role, user.plan)
        response = RedirectResponse(url="/", status_code=302)
        response.set_cookie(SESSION_COOKIE, token, httponly=True, secure=True, samesite="lax", max_age=86400)
        return response

    _record_login_attempt(client_ip)
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
    empresa = form.get("empresa", "").strip()
    empresa_cif = form.get("empresa_cif", "").strip().upper()
    telefono = form.get("telefono", "").strip()
    password = form.get("password", "")
    password2 = form.get("password2", "")

    form_data = {"email": email, "nombre": nombre, "empresa": empresa,
                 "empresa_cif": empresa_cif, "telefono": telefono}

    if not all([email, nombre, empresa, empresa_cif, telefono, password]):
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Todos los campos marcados con * son obligatorios",
            **form_data,
        }, status_code=400)

    if password != password2:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Las contrasenas no coinciden",
            **form_data,
        }, status_code=400)

    if len(password) < 6:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "La contrasena debe tener al menos 6 caracteres",
            **form_data,
        }, status_code=400)

    # Validate CIF format (letter + 8 digits, or 8 digits + letter)
    import re
    if not re.match(r'^[A-Z]\d{8}$|^\d{8}[A-Z]$', empresa_cif):
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "CIF no valido. Formato: B12345678",
            **form_data,
        }, status_code=400)

    # Check if email exists
    from sqlalchemy import select
    from app.db.models import User
    existing = await db.scalar(select(User).where(User.email == email))
    if existing:
        return templates.TemplateResponse("register.html", {
            "request": request, "error": "Ya existe una cuenta con ese email",
            **form_data,
        }, status_code=400)

    user = await create_user(email, nombre, password, db,
                             empresa=empresa, empresa_cif=empresa_cif, telefono=telefono)

    # Send verification email
    from app.services.email_service import generate_code, send_verification_email
    from datetime import datetime as _dt
    code = generate_code()
    user.verification_code = code
    user.verification_code_at = _dt.utcnow()
    await db.commit()
    await send_verification_email(email, code, nombre)

    # Redirect to verification page (don't login yet)
    return templates.TemplateResponse("verify_email.html", {
        "request": request, "email": email,
    })


@web_router.post("/verify-email")
async def verify_email_submit(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    email = form.get("email", "").strip().lower()
    code = form.get("code", "").strip().upper()

    from sqlalchemy import select
    from app.db.models import User
    from datetime import datetime as _dt, timedelta
    user = await db.scalar(select(User).where(User.email == email))

    # Generic error to prevent account enumeration
    _generic_error = "Codigo incorrecto o expirado"

    if not user or user.email_verified:
        return templates.TemplateResponse("verify_email.html", {
            "request": request, "email": email, "error": _generic_error,
        }, status_code=400)

    # Check code expiry (30 minutes)
    if user.verification_code_at:
        elapsed = _dt.utcnow() - user.verification_code_at
        if elapsed > timedelta(minutes=30):
            return templates.TemplateResponse("verify_email.html", {
                "request": request, "email": email,
                "error": "Codigo expirado. Solicita uno nuevo.",
            }, status_code=400)

    if not user.verification_code or user.verification_code != code:
        return templates.TemplateResponse("verify_email.html", {
            "request": request, "email": email, "error": _generic_error,
        }, status_code=400)

    # Mark as verified and login
    user.email_verified = True
    user.verification_code = None
    user.verification_code_at = None
    await db.commit()

    token = create_session(user.id, user.email, user.role, user.plan)
    response = RedirectResponse(url="/", status_code=302)
    response.set_cookie(SESSION_COOKIE, token, httponly=True, secure=True, samesite="lax", max_age=86400)
    return response


@web_router.post("/verify-email/resend")
async def verify_email_resend(request: Request, db: AsyncSession = Depends(get_db)):
    form = await request.form()
    email = form.get("email", "").strip().lower()

    from sqlalchemy import select
    from app.db.models import User
    user = await db.scalar(select(User).where(User.email == email))

    if user and not user.email_verified:
        from datetime import datetime as _dt, timedelta
        # Rate limit: only allow resend every 60 seconds
        if user.verification_code_at:
            elapsed = _dt.utcnow() - user.verification_code_at
            if elapsed < timedelta(seconds=60):
                return templates.TemplateResponse("verify_email.html", {
                    "request": request, "email": email,
                    "error": "Espera un minuto antes de reenviar.",
                })
        from app.services.email_service import generate_code, send_verification_email
        code = generate_code()
        user.verification_code = code
        user.verification_code_at = _dt.utcnow()
        await db.commit()
        await send_verification_email(email, code, user.nombre)

    # Always show success to prevent enumeration
    return templates.TemplateResponse("verify_email.html", {
        "request": request, "email": email,
        "success": "Si la cuenta existe, se ha reenviado el codigo.",
    })


@web_router.get("/pricing", response_class=HTMLResponse)
async def pricing_page(request: Request):
    # Check auth manually since /pricing is a public path
    user = get_current_user(request)
    return templates.TemplateResponse("pricing.html", {"request": request, "user": user})


@web_router.get("/legal/terminos", response_class=HTMLResponse)
async def legal_terminos(request: Request):
    return templates.TemplateResponse("legal_terminos.html", _ctx(request, active_page="legal"))


@web_router.get("/legal/privacidad", response_class=HTMLResponse)
async def legal_privacidad(request: Request):
    return templates.TemplateResponse("legal_privacidad.html", _ctx(request, active_page="legal"))


@web_router.get("/legal/cookies", response_class=HTMLResponse)
async def legal_cookies(request: Request):
    return templates.TemplateResponse("legal_cookies.html", _ctx(request, active_page="legal"))


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


# --- Admin ---

@web_router.get("/admin/users", response_class=HTMLResponse)
async def admin_users_page(request: Request, db: AsyncSession = Depends(get_db)):
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/", status_code=302)
    from sqlalchemy import select
    from app.db.models import User
    result = await db.scalars(select(User).order_by(User.created_at.desc()))
    users = result.all()
    return templates.TemplateResponse("admin_users.html", _ctx(
        request, users=users, active_page="admin",
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
    score_min: str | None = None,
    sort_by: str = "fecha_ultima_publicacion",
    sort_order: str = "desc",  # Always desc
    page: int = 1,
    per_page: int = 25,
    db: AsyncSession = Depends(get_db),
):
    # Convert empty strings to None for optional numeric fields
    _score_min = int(score_min) if score_min and score_min.strip() else None
    # Check search limits for free users
    user = getattr(request.state, "user", None)
    if user and page == 1:  # Only count on first page (new search)
        limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
        if limits["searches"] != -1:
            from app.db.models import User
            from datetime import datetime as _dt
            db_user = await db.get(User, user["user_id"])
            if db_user:
                current_month = _dt.now().strftime("%Y-%m")
                if db_user.month_reset != current_month:
                    db_user.searches_this_month = 0
                    db_user.exports_this_month = 0
                    db_user.month_reset = current_month
                if db_user.searches_this_month >= limits["searches"]:
                    return HTMLResponse(
                        '<div class="text-center py-8 text-red-500 text-sm font-medium">'
                        f'Has alcanzado el limite de {limits["searches"]} busquedas/mes. '
                        '<a href="/pricing" class="underline">Mejora tu plan</a> para busquedas ilimitadas.</div>'
                    )
                db_user.searches_this_month += 1
                await db.commit()

    filters = SearchFilters(
        q=q or None, cif=cif or None, provincia=provincia or None,
        forma_juridica=forma_juridica or None, cnae_code=cnae_code or None,
        estado=estado or None,
        pub_desde=pub_desde or None, pub_hasta=pub_hasta or None,
        score_min=_score_min, sort_by=sort_by, sort_order="desc",
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
    user = get_current_user(request)
    user_id = user["user_id"] if user else None

    # --- Usage metering: count detail views ---
    if user_id:
        from app.db.models import User
        from datetime import datetime
        db_user = await db.get(User, user_id)
        if db_user:
            current_month = datetime.now().strftime("%Y-%m")
            if db_user.month_reset != current_month:
                db_user.searches_this_month = 0
                db_user.exports_this_month = 0
                db_user.detail_views_this_month = 0
                db_user.month_reset = current_month
            # Check limit
            limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
            if limits["detail_views"] != -1 and db_user.detail_views_this_month >= limits["detail_views"]:
                return templates.TemplateResponse("limit_reached.html", _ctx(
                    request, limit_type="detail_views", limit=limits["detail_views"],
                    active_page="search",
                ))
            db_user.detail_views_this_month += 1
            await db.commit()

    company = await get_company(company_id, db)
    if not company:
        return HTMLResponse("<h1>Empresa no encontrada</h1>", status_code=404)
    watched = await is_watched(company_id, db, user_id=user_id)
    from app.utils.cnae import get_cnae_description
    cnae_desc = get_cnae_description(company.cnae_code) if company.cnae_code else None
    return templates.TemplateResponse("company_detail.html", _ctx(
        request, company=company, watched=watched, active_page="search",
        cnae_desc=cnae_desc,
    ))


# --- Ingestion ---

@web_router.get("/ingestion", response_class=HTMLResponse)
async def ingestion_page(request: Request, db: AsyncSession = Depends(get_db)):
    user = getattr(request.state, "user", None)
    if not user or user.get("role") != "admin":
        return RedirectResponse(url="/", status_code=302)
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
    act_type_watches = await get_act_type_watches(user_id, db) if user_id else []
    provinces = get_all_provinces()
    from app.services.borme_parser import ACT_TYPES
    return templates.TemplateResponse("watchlist.html", _ctx(
        request, unread_count=unread, active_page="watchlist",
        act_type_watches=act_type_watches, provinces=provinces, act_types=ACT_TYPES,
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
