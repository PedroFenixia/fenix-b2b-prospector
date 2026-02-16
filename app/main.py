from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.auth import get_current_user
from app.config import settings
from app.db.engine import engine
from app.db.models import Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    settings.ensure_dirs()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Seed reference data
    from app.db.seed_cnae import seed_all
    await seed_all()

    # Seed default users (admin + demo)
    from app.db.engine import async_session
    from app.auth import seed_default_users
    async with async_session() as db:
        await seed_default_users(db)

    # Start daily scheduler
    if settings.scheduler_enabled:
        from app.scheduler import start_scheduler
        start_scheduler(hour=settings.scheduler_hour, minute=settings.scheduler_minute)

    yield

    # Shutdown scheduler
    if settings.scheduler_enabled:
        from app.scheduler import stop_scheduler
        stop_scheduler()

    await engine.dispose()


app = FastAPI(
    title="FENIX Prospector",
    description="Lead prospecting from Spanish public registries (BOE/BORME)",
    version="0.2.0",
    lifespan=lifespan,
)

# Session middleware for auth
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)

# Static files
static_dir = Path(__file__).parent / "web" / "static"
static_dir.mkdir(parents=True, exist_ok=True)
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# API routes
from app.api.router import api_router  # noqa: E402
app.include_router(api_router, prefix="/api")

# Web routes (server-rendered HTML)
from app.web.routes import web_router  # noqa: E402
app.include_router(web_router)


# Public paths that don't require auth
PUBLIC_PATHS = ["/login", "/register", "/health", "/static/", "/favicon.ico", "/pricing",
                "/api/billing/webhook"]


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Protect all routes. Extract user info into request.state."""
    path = request.url.path

    # Allow public paths
    if any(path.startswith(p) for p in PUBLIC_PATHS):
        request.state.user = None
        return await call_next(request)

    # Check auth
    user_data = get_current_user(request)
    if not user_data:
        # API routes return 401, web routes redirect
        if path.startswith("/api/"):
            from fastapi.responses import JSONResponse
            return JSONResponse({"error": "No autenticado"}, status_code=401)
        return RedirectResponse(url="/login", status_code=302)

    # Inject user into request state
    request.state.user = user_data
    return await call_next(request)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "fenix-prospector"}
