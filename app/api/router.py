from fastapi import APIRouter

from app.api.companies import router as companies_router
from app.api.export import router as export_router
from app.api.ingestion import router as ingestion_router
from app.api.opportunities import router as opportunities_router
from app.api.search import router as search_router
from app.api.stats import router as stats_router
from app.api.watchlist import router as watchlist_router
from app.api.billing import router as billing_router
from app.api.solvency import router as solvency_router
from app.api.erp import router as erp_router

api_router = APIRouter()
api_router.include_router(companies_router, prefix="/companies", tags=["companies"])
api_router.include_router(search_router, prefix="/search", tags=["search"])
api_router.include_router(opportunities_router, prefix="/opportunities", tags=["opportunities"])
api_router.include_router(export_router, prefix="/export", tags=["export"])
api_router.include_router(ingestion_router, prefix="/ingestion", tags=["ingestion"])
api_router.include_router(stats_router, prefix="/stats", tags=["stats"])
api_router.include_router(watchlist_router, prefix="/watchlist", tags=["watchlist"])
api_router.include_router(billing_router, prefix="/billing", tags=["billing"])
api_router.include_router(solvency_router, prefix="/solvency", tags=["solvency"])
api_router.include_router(erp_router, prefix="/erp", tags=["erp"])
