from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel, Field


class ScoreDetail(BaseModel):
    estado: str
    antiguedad: str
    capital: str
    contacto: str
    actos_riesgo: str
    estabilidad: str
    judicial: str


class SolvencyResponse(BaseModel):
    cif: str
    nombre: str
    estado: str
    forma_juridica: str | None = None
    provincia: str | None = None
    capital_social: float | None = None
    fecha_constitucion: date | None = None
    score: int
    risk_level: str
    score_detail: ScoreDetail
    score_computed_at: datetime | None = None


class SolvencyBatchRequest(BaseModel):
    cifs: list[str] = Field(..., min_length=1, max_length=50)


class SolvencyBatchResponse(BaseModel):
    results: list[SolvencyResponse]
    not_found: list[str]
