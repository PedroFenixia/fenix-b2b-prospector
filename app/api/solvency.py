"""API REST de solvencia para integracion con ERPs.

Permite consultar el score de solvencia de una empresa por CIF.
Requiere autenticacion via X-API-Key header.
"""
from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import get_api_key_user
from app.db.engine import get_db
from app.db.models import Act, Company, JudicialNotice, Officer
from app.schemas.solvency import (
    ScoreDetail,
    SolvencyBatchRequest,
    SolvencyBatchResponse,
    SolvencyResponse,
)
from app.services.scoring_service import compute_score_detailed

router = APIRouter()


async def _require_api_key(request: Request, db: AsyncSession = Depends(get_db)) -> dict:
    """Dependency: requiere API key valida."""
    user = await get_api_key_user(request, db)
    if not user:
        raise HTTPException(status_code=401, detail="API key invalida o ausente")
    return user


async def _build_solvency(company: Company, db: AsyncSession) -> SolvencyResponse:
    """Construye SolvencyResponse para una empresa."""
    # Load acts
    acts = (await db.scalars(
        select(Act).where(Act.company_id == company.id)
    )).all()

    # Load officers
    officers = (await db.scalars(
        select(Officer).where(Officer.company_id == company.id)
    )).all()

    # Check judicial
    has_judicial = False
    if company.nombre_normalizado:
        jcount = await db.scalar(
            select(JudicialNotice.id).where(
                JudicialNotice.deudor.contains(company.nombre_normalizado[:30])
            ).limit(1)
        )
        has_judicial = jcount is not None

    result = compute_score_detailed(company, list(acts), list(officers), has_judicial)

    # Persist score
    company.score_solvencia = result["score"]
    company.score_updated_at = datetime.utcnow()
    await db.commit()

    return SolvencyResponse(
        cif=company.cif or "",
        nombre=company.nombre,
        estado=company.estado or "activa",
        forma_juridica=company.forma_juridica,
        provincia=company.provincia,
        capital_social=company.capital_social,
        fecha_constitucion=company.fecha_constitucion,
        score=result["score"],
        risk_level=result["risk_level"],
        score_detail=ScoreDetail(**result["detail"]),
        score_computed_at=company.score_updated_at,
    )


@router.get("/check", response_model=SolvencyResponse)
async def check_solvency(
    cif: str,
    user: dict = Depends(_require_api_key),
    db: AsyncSession = Depends(get_db),
):
    """Consulta solvencia por CIF."""
    cif_clean = cif.strip().upper()
    company = await db.scalar(
        select(Company).where(Company.cif == cif_clean)
    )
    if not company:
        raise HTTPException(status_code=404, detail=f"Empresa con CIF {cif_clean} no encontrada")

    return await _build_solvency(company, db)


@router.post("/batch", response_model=SolvencyBatchResponse)
async def batch_solvency(
    body: SolvencyBatchRequest,
    user: dict = Depends(_require_api_key),
    db: AsyncSession = Depends(get_db),
):
    """Consulta solvencia para varios CIFs (max 50)."""
    results: list[SolvencyResponse] = []
    not_found: list[str] = []

    for cif in body.cifs:
        cif_clean = cif.strip().upper()
        company = await db.scalar(
            select(Company).where(Company.cif == cif_clean)
        )
        if company:
            resp = await _build_solvency(company, db)
            results.append(resp)
        else:
            not_found.append(cif_clean)

    return SolvencyBatchResponse(results=results, not_found=not_found)
