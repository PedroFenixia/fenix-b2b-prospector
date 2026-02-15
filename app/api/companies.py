from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.schemas.company import ActOut, CompanyDetail, CompanyOut, OfficerOut
from app.services.company_service import (
    get_company,
    get_company_acts,
    get_company_officers,
)

router = APIRouter()


@router.get("/{company_id}", response_model=CompanyDetail)
async def read_company(company_id: int, db: AsyncSession = Depends(get_db)):
    company = await get_company(company_id, db)
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return company


@router.get("/{company_id}/acts", response_model=list[ActOut])
async def read_company_acts(company_id: int, db: AsyncSession = Depends(get_db)):
    return await get_company_acts(company_id, db)


@router.get("/{company_id}/officers", response_model=list[OfficerOut])
async def read_company_officers(company_id: int, db: AsyncSession = Depends(get_db)):
    return await get_company_officers(company_id, db)
