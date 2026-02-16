from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.db.models import Act, Company, IngestionLog, JudicialNotice, Officer, Subsidy, Tender

router = APIRouter()


@router.get("")
async def get_stats(db: AsyncSession = Depends(get_db)):
    """Dashboard statistics."""
    total_companies = await db.scalar(select(func.count(Company.id))) or 0
    total_acts = await db.scalar(select(func.count(Act.id))) or 0
    total_officers = await db.scalar(select(func.count(Officer.id))) or 0
    total_subsidies = await db.scalar(select(func.count(Subsidy.id))) or 0
    total_tenders = await db.scalar(select(func.count(Tender.id))) or 0
    total_judicial = await db.scalar(select(func.count(JudicialNotice.id))) or 0

    # Companies by province (top 15)
    prov_query = (
        select(Company.provincia, func.count(Company.id))
        .where(Company.provincia.isnot(None))
        .group_by(Company.provincia)
        .order_by(func.count(Company.id).desc())
        .limit(15)
    )
    prov_result = await db.execute(prov_query)
    by_province = {row[0]: row[1] for row in prov_result.all()}

    # Companies by forma juridica
    forma_query = (
        select(Company.forma_juridica, func.count(Company.id))
        .where(Company.forma_juridica.isnot(None))
        .group_by(Company.forma_juridica)
        .order_by(func.count(Company.id).desc())
    )
    forma_result = await db.execute(forma_query)
    by_forma = {row[0]: row[1] for row in forma_result.all()}

    # Companies by estado
    estado_query = (
        select(Company.estado, func.count(Company.id))
        .group_by(Company.estado)
    )
    estado_result = await db.execute(estado_query)
    by_estado = {row[0]: row[1] for row in estado_result.all()}

    # Recent incorporations
    recent_query = (
        select(Company)
        .join(Act)
        .where(Act.tipo_acto == "Constitución")
        .order_by(Company.fecha_ultima_publicacion.desc())
        .limit(10)
    )
    recent_result = await db.scalars(recent_query)
    recent = [
        {
            "id": c.id,
            "nombre": c.nombre,
            "provincia": c.provincia,
            "fecha": str(c.fecha_ultima_publicacion),
            "capital": c.capital_social,
        }
        for c in recent_result.all()
    ]

    # Ingestion coverage
    first_date = await db.scalar(
        select(func.min(IngestionLog.fecha_borme)).where(IngestionLog.status == "completed")
    )
    last_date = await db.scalar(
        select(func.max(IngestionLog.fecha_borme)).where(IngestionLog.status == "completed")
    )
    total_days = await db.scalar(
        select(func.count(IngestionLog.id)).where(IngestionLog.status == "completed")
    ) or 0

    return {
        "total_companies": total_companies,
        "total_acts": total_acts,
        "total_officers": total_officers,
        "total_subsidies": total_subsidies,
        "total_tenders": total_tenders,
        "total_judicial": total_judicial,
        "companies_by_province": by_province,
        "companies_by_forma": by_forma,
        "companies_by_estado": by_estado,
        "recent_incorporations": recent,
        "ingestion_coverage": {
            "first_date": str(first_date) if first_date else None,
            "last_date": str(last_date) if last_date else None,
            "total_days": total_days,
        },
    }
