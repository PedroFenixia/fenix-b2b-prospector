from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import PLAN_LIMITS
from app.db.engine import get_db
from app.schemas.company import ActOut, CompanyDetail, CompanyOut, OfficerOut
from app.services.company_service import (
    get_company,
    get_company_acts,
    get_company_officers,
    update_company_cif,
)

router = APIRouter()


class UpdateCifRequest(BaseModel):
    cif: str | None = None


@router.get("/{company_id}", response_model=CompanyDetail)
async def read_company(company_id: int, db: AsyncSession = Depends(get_db)):
    company = await get_company(company_id, db)
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return company


@router.patch("/{company_id}/cif", response_model=CompanyOut)
async def patch_company_cif(
    company_id: int,
    body: UpdateCifRequest,
    db: AsyncSession = Depends(get_db),
):
    company = await update_company_cif(company_id, body.cif, db)
    if not company:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return company


@router.post("/{company_id}/lookup-cif")
async def lookup_company_cif(company_id: int, db: AsyncSession = Depends(get_db)):
    """Lookup CIF for a company searching the web."""
    from app.services.cif_enrichment import enrich_company_cif
    cif = await enrich_company_cif(company_id, db)
    if cif:
        return {"cif": cif}
    return {"cif": None, "error": "CIF no encontrado"}


@router.post("/{company_id}/enrich-web")
async def enrich_company_web(company_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """Search web for company CIF, email, phone."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "Inicia sesion para usar el enriquecimiento."}, status_code=401)

    limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
    if not limits.get("enrichment"):
        return JSONResponse({"error": "Enriquecimiento no disponible en tu plan. Mejora a Pro."}, status_code=403)

    enrichment_limit = limits.get("enrichment_limit", 0)
    if enrichment_limit != -1:
        from app.db.models import User
        from datetime import datetime as _dt
        db_user = await db.get(User, user["user_id"])
        if db_user:
            current_month = _dt.now().strftime("%Y-%m")
            if db_user.month_reset != current_month:
                db_user.exports_this_month = 0
                db_user.enrichments_this_month = 0
                db_user.month_reset = current_month
            if db_user.enrichments_this_month >= enrichment_limit:
                return JSONResponse(
                    {"error": f"Has alcanzado el limite de {enrichment_limit} enriquecimientos/mes."},
                    status_code=403,
                )
            db_user.enrichments_this_month += 1
            await db.commit()

    from app.services.web_enrichment import enrich_single_web
    result = await enrich_single_web(company_id, db)
    return result


@router.post("/{company_id}/score")
async def compute_company_score(company_id: int, request: Request, db: AsyncSession = Depends(get_db)):
    """Compute solvency score for a company."""
    user = getattr(request.state, "user", None)
    if not user:
        return JSONResponse({"error": "Inicia sesion para usar el scoring."}, status_code=401)

    limits = PLAN_LIMITS.get(user.get("plan", "free"), PLAN_LIMITS["free"])
    if not limits.get("scoring"):
        return JSONResponse({"error": "Scoring no disponible en tu plan. Mejora a Pro."}, status_code=403)

    from app.services.scoring_service import score_company
    score = await score_company(company_id, db)
    if score is None:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return {"score": score}


@router.get("/{company_id}/acts", response_model=list[ActOut])
async def read_company_acts(company_id: int, db: AsyncSession = Depends(get_db)):
    return await get_company_acts(company_id, db)


@router.get("/{company_id}/officers", response_model=list[OfficerOut])
async def read_company_officers(company_id: int, db: AsyncSession = Depends(get_db)):
    return await get_company_officers(company_id, db)
