from __future__ import annotations

from fastapi import APIRouter, Depends
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.schemas.search import SearchFilters
from app.services.export_service import export_csv, export_excel

router = APIRouter()


@router.get("/csv")
async def export_to_csv(
    q: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export filtered companies as CSV."""
    filters = SearchFilters(
        q=q,
        provincia=provincia,
        forma_juridica=forma_juridica,
        cnae_code=cnae_code,
        estado=estado,
        pub_desde=pub_desde,
        pub_hasta=pub_hasta,
    )
    filepath = await export_csv(filters, db)
    return FileResponse(
        filepath,
        media_type="text/csv",
        filename=filepath.name,
        headers={"Content-Disposition": f"attachment; filename={filepath.name}"},
    )


@router.get("/excel")
async def export_to_excel(
    q: str | None = None,
    provincia: str | None = None,
    forma_juridica: str | None = None,
    cnae_code: str | None = None,
    estado: str | None = None,
    pub_desde: str | None = None,
    pub_hasta: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    """Export filtered companies as Excel."""
    filters = SearchFilters(
        q=q,
        provincia=provincia,
        forma_juridica=forma_juridica,
        cnae_code=cnae_code,
        estado=estado,
        pub_desde=pub_desde,
        pub_hasta=pub_hasta,
    )
    filepath = await export_excel(filters, db)
    return FileResponse(
        filepath,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=filepath.name,
        headers={"Content-Disposition": f"attachment; filename={filepath.name}"},
    )
