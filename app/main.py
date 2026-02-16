from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from app.auth import is_authenticated
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
    version="0.1.0",
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


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """Protect all web routes except login, health, static, and API."""
    path = request.url.path

    # Allow public paths
    public = ["/login", "/health", "/static/", "/api/", "/favicon.ico"]
    if any(path.startswith(p) for p in public):
        return await call_next(request)

    if not is_authenticated(request):
        return RedirectResponse(url="/login", status_code=302)

    return await call_next(request)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "fenix-prospector"}
