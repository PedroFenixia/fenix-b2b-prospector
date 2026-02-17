"""Sistema de puntuación de solvencia empresarial.

Score 0-100 basado en señales extraídas del BORME y datos disponibles.
- 0-25:  Riesgo muy alto (rojo)
- 26-50: Riesgo moderado (naranja)
- 51-75: Fiable (amarillo-verde)
- 76-100: Muy fiable (verde)
"""
from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Optional

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.models import Act, Company, JudicialNotice, Officer

logger = logging.getLogger(__name__)

# Act types that signal risk
RISK_ACT_TYPES = {
    "Disolución",
    "Disolucion",
    "Liquidación",
    "Liquidacion",
    "Concurso",
    "Situación concursal",
    "Revocaciones",
    "Extinción",
    "Extincion",
}

CAPITAL_REDUCTION_TYPES = {
    "Reducción de capital",
    "Reduccion de capital",
}

ADMIN_CHANGE_TYPES = {
    "Ceses/Dimisiones",
    "Ceses",
    "Dimisiones",
}


def compute_score(
    company: Company,
    acts: list[Act],
    officers: list[Officer],
    has_judicial: bool = False,
) -> int:
    """Compute solvency score (0-100) for a company based on available data.

    Base: 50 points. Signals add or subtract.
    """
    score = 50.0
    today = date.today()

    # --- ESTADO ---
    estado = (company.estado or "activa").lower()
    if estado == "activa":
        score += 10
    elif estado == "disuelta":
        score -= 30
    elif estado == "en_liquidacion":
        score -= 25
    elif estado == "extinguida":
        score -= 40

    # --- ANTIGÜEDAD ---
    ref_date = company.fecha_constitucion or company.fecha_primera_publicacion
    if ref_date:
        years = (today - ref_date).days / 365.25
        if years > 10:
            score += 15
        elif years > 5:
            score += 10
        elif years > 2:
            score += 5
        elif years > 1:
            score += 2
        elif years < 0.5:
            score -= 5  # Very new

    # --- CAPITAL SOCIAL ---
    capital = company.capital_social or 0
    if capital >= 500_000:
        score += 12
    elif capital >= 100_000:
        score += 10
    elif capital >= 30_000:
        score += 7
    elif capital >= 10_000:
        score += 5
    elif capital > 3_000:
        score += 2
    elif capital == 3_000:
        score -= 2  # Minimum SL capital
    # capital 0 or unknown: no change

    # --- DATOS DE CONTACTO (señales de transparencia) ---
    if company.cif:
        score += 3
    if company.web:
        score += 2
    if company.email or company.telefono:
        score += 2

    # --- ANÁLISIS DE ACTOS ---
    risk_acts = 0
    capital_changes = 0
    for act in acts:
        tipo = act.tipo_acto or ""
        if any(r in tipo for r in RISK_ACT_TYPES):
            risk_acts += 1
        if any(r in tipo for r in CAPITAL_REDUCTION_TYPES):
            capital_changes += 1

    if risk_acts > 0:
        score -= min(risk_acts * 8, 20)
    if capital_changes > 2:
        score -= 5

    # --- ESTABILIDAD DIRECTIVA ---
    two_years_ago = date(today.year - 2, today.month, today.day)
    recent_ceses = sum(
        1 for o in officers
        if o.tipo_evento in ("cese", "dimision", "dimisión", "revocacion", "revocación")
        and o.fecha_publicacion >= two_years_ago
    )
    if recent_ceses == 0:
        score += 5  # Very stable
    elif recent_ceses > 5:
        score -= 10
    elif recent_ceses > 3:
        score -= 5

    # --- AVISOS JUDICIALES ---
    if has_judicial:
        score -= 20

    # Clamp to 0-100
    return max(0, min(100, round(score)))


def compute_score_detailed(
    company: Company,
    acts: list[Act],
    officers: list[Officer],
    has_judicial: bool = False,
) -> dict:
    """Compute score with per-factor breakdown for API responses."""
    detail = {}
    score = 50.0
    today = date.today()

    # Estado
    estado = (company.estado or "activa").lower()
    pts = {
        "activa": 10, "disuelta": -30, "en_liquidacion": -25, "extinguida": -40,
    }.get(estado, 0)
    score += pts
    detail["estado"] = f"{pts:+d}"

    # Antiguedad
    ref_date = company.fecha_constitucion or company.fecha_primera_publicacion
    pts = 0
    if ref_date:
        years = (today - ref_date).days / 365.25
        if years > 10:
            pts = 15
        elif years > 5:
            pts = 10
        elif years > 2:
            pts = 5
        elif years > 1:
            pts = 2
        elif years < 0.5:
            pts = -5
    score += pts
    detail["antiguedad"] = f"{pts:+d}"

    # Capital social
    capital = company.capital_social or 0
    pts = 0
    if capital >= 500_000:
        pts = 12
    elif capital >= 100_000:
        pts = 10
    elif capital >= 30_000:
        pts = 7
    elif capital >= 10_000:
        pts = 5
    elif capital > 3_000:
        pts = 2
    elif capital == 3_000:
        pts = -2
    score += pts
    detail["capital"] = f"{pts:+d}"

    # Contacto
    pts = 0
    if company.cif:
        pts += 3
    if company.web:
        pts += 2
    if company.email or company.telefono:
        pts += 2
    score += pts
    detail["contacto"] = f"{pts:+d}"

    # Actos de riesgo
    risk_acts = sum(1 for act in acts if any(r in (act.tipo_acto or "") for r in RISK_ACT_TYPES))
    capital_changes = sum(1 for act in acts if any(r in (act.tipo_acto or "") for r in CAPITAL_REDUCTION_TYPES))
    pts = 0
    if risk_acts > 0:
        pts -= min(risk_acts * 8, 20)
    if capital_changes > 2:
        pts -= 5
    score += pts
    detail["actos_riesgo"] = f"{pts:+d}"

    # Estabilidad directiva
    two_years_ago = date(today.year - 2, today.month, today.day)
    recent_ceses = sum(
        1 for o in officers
        if o.tipo_evento in ("cese", "dimision", "dimisión", "revocacion", "revocación")
        and o.fecha_publicacion >= two_years_ago
    )
    pts = 0
    if recent_ceses == 0:
        pts = 5
    elif recent_ceses > 5:
        pts = -10
    elif recent_ceses > 3:
        pts = -5
    score += pts
    detail["estabilidad"] = f"{pts:+d}"

    # Judicial
    pts = -20 if has_judicial else 0
    score += pts
    detail["judicial"] = f"{pts:+d}"

    final_score = max(0, min(100, round(score)))

    # Risk level
    if final_score >= 76:
        risk_level = "muy_fiable"
    elif final_score >= 51:
        risk_level = "fiable"
    elif final_score >= 26:
        risk_level = "riesgo_moderado"
    else:
        risk_level = "riesgo_alto"

    return {"score": final_score, "risk_level": risk_level, "detail": detail}


async def score_company(company_id: int, db: AsyncSession) -> Optional[int]:
    """Compute and store solvency score for a single company."""
    company = await db.get(Company, company_id)
    if not company:
        return None

    # Load acts
    acts_result = await db.scalars(
        select(Act).where(Act.company_id == company_id)
    )
    acts = acts_result.all()

    # Load officers
    officers_result = await db.scalars(
        select(Officer).where(Officer.company_id == company_id)
    )
    officers = officers_result.all()

    # Check judicial notices (match by normalized name)
    has_judicial = False
    if company.nombre_normalizado:
        judicial_count = await db.scalar(
            select(func.count(JudicialNotice.id)).where(
                JudicialNotice.deudor.contains(company.nombre_normalizado[:30])
            )
        )
        has_judicial = (judicial_count or 0) > 0

    score = compute_score(company, acts, officers, has_judicial)
    company.score_solvencia = score
    company.score_updated_at = datetime.utcnow()
    await db.commit()

    logger.info(f"Score: {company.nombre} -> {score}/100")
    return score


async def score_batch(
    db: AsyncSession,
    limit: int = 500,
    only_unscored: bool = True,
) -> dict:
    """Score a batch of companies. Returns stats."""
    query = select(Company)
    if only_unscored:
        query = query.where(Company.score_solvencia.is_(None))
    query = query.order_by(Company.fecha_ultima_publicacion.desc()).limit(limit)

    companies = (await db.scalars(query)).all()

    stats = {"attempted": 0, "scored": 0, "errors": 0}

    for company in companies:
        stats["attempted"] += 1
        try:
            # Load acts for this company
            acts = (await db.scalars(
                select(Act).where(Act.company_id == company.id)
            )).all()

            officers = (await db.scalars(
                select(Officer).where(Officer.company_id == company.id)
            )).all()

            has_judicial = False
            if company.nombre_normalizado:
                jcount = await db.scalar(
                    select(func.count(JudicialNotice.id)).where(
                        JudicialNotice.deudor.contains(
                            company.nombre_normalizado[:30]
                        )
                    )
                )
                has_judicial = (jcount or 0) > 0

            score = compute_score(company, acts, officers, has_judicial)
            company.score_solvencia = score
            company.score_updated_at = datetime.utcnow()
            stats["scored"] += 1

        except Exception as e:
            logger.error(f"Scoring error for {company.nombre}: {e}")
            stats["errors"] += 1

    await db.commit()
    return stats


async def get_score_stats(db: AsyncSession) -> dict:
    """Get scoring coverage stats."""
    total = await db.scalar(select(func.count(Company.id)))
    scored = await db.scalar(
        select(func.count(Company.id)).where(Company.score_solvencia.isnot(None))
    )
    avg_score = await db.scalar(
        select(func.avg(Company.score_solvencia)).where(
            Company.score_solvencia.isnot(None)
        )
    )

    # Distribution
    high = await db.scalar(
        select(func.count(Company.id)).where(Company.score_solvencia >= 76)
    )
    medium = await db.scalar(
        select(func.count(Company.id)).where(
            Company.score_solvencia >= 51, Company.score_solvencia < 76
        )
    )
    low = await db.scalar(
        select(func.count(Company.id)).where(
            Company.score_solvencia >= 26, Company.score_solvencia < 51
        )
    )
    very_low = await db.scalar(
        select(func.count(Company.id)).where(
            Company.score_solvencia < 26, Company.score_solvencia.isnot(None)
        )
    )

    return {
        "total": total or 0,
        "scored": scored or 0,
        "unscored": (total or 0) - (scored or 0),
        "coverage_pct": round((scored or 0) / total * 100, 1) if total else 0,
        "avg_score": round(avg_score, 1) if avg_score else None,
        "distribution": {
            "muy_fiable": high or 0,
            "fiable": medium or 0,
            "riesgo_moderado": low or 0,
            "riesgo_alto": very_low or 0,
        },
    }
