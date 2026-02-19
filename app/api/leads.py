"""Inbound leads from FENIX IA 360 landing pages."""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel, EmailStr
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import get_db
from app.db.models import InboundLead, Company

logger = logging.getLogger(__name__)

router = APIRouter()


class InboundLeadIn(BaseModel):
    name: str
    email: str
    company: Optional[str] = None
    employees: Optional[int] = None
    product: Optional[str] = None
    source: Optional[str] = None


@router.post("/inbound")
async def create_inbound_lead(payload: InboundLeadIn, db: AsyncSession = Depends(get_db)):
    """Receive a lead from a landing page form and optionally match to a BORME company."""

    matched_id = None
    if payload.company:
        # Try to match against known companies by normalized name
        normalized = payload.company.strip().upper()
        result = await db.execute(
            select(Company.id)
            .where(Company.nombre_normalizado == normalized)
            .limit(1)
        )
        row = result.scalar_one_or_none()
        if row:
            matched_id = row

    lead = InboundLead(
        name=payload.name,
        email=payload.email,
        company=payload.company,
        employees=payload.employees,
        product=payload.product,
        source=payload.source,
        matched_company_id=matched_id,
    )
    db.add(lead)
    await db.commit()

    logger.info("Inbound lead created: %s (%s) from %s", payload.email, payload.company, payload.source)
    return {"ok": True, "matched": matched_id is not None}
