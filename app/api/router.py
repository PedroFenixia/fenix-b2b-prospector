from fastapi import APIRouter

from app.api.companies import router as companies_router
from app.api.export import router as export_router
from app.api.ingestion import router as ingestion_router
from app.api.search import router as search_router
from app.api.stats import router as stats_router

api_router = APIRouter()
api_router.include_router(companies_router, prefix="/companies", tags=["companies"])
api_router.include_router(search_router, prefix="/search", tags=["search"])
api_router.include_router(export_router, prefix="/export", tags=["export"])
api_router.include_router(ingestion_router, prefix="/ingestion", tags=["ingestion"])
api_router.include_router(stats_router, prefix="/stats", tags=["stats"])
