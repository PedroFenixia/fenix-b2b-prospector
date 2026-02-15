from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

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

    yield
    # Shutdown
    await engine.dispose()


app = FastAPI(
    title="FENIX Prospector",
    description="Lead prospecting from Spanish public registries (BOE/BORME)",
    version="0.1.0",
    lifespan=lifespan,
)

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


@app.get("/health")
async def health():
    return {"status": "ok", "service": "fenix-prospector"}
