from __future__ import annotations

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field


class SubsidyOut(BaseModel):
    id: int
    boe_id: str
    titulo: str
    organismo: str | None = None
    descripcion: str | None = None
    url_html: str | None = None
    url_pdf: str | None = None
    fecha_publicacion: date
    fecha_limite: date | None = None
    importe: float | None = None
    beneficiarios: str | None = None
    sector: str | None = None
    ambito: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class TenderOut(BaseModel):
    id: int
    expediente: str
    titulo: str
    organismo: str | None = None
    estado: str | None = None
    tipo_contrato: str | None = None
    descripcion: str | None = None
    url_licitacion: str | None = None
    fecha_publicacion: date
    fecha_limite: date | None = None
    importe_estimado: float | None = None
    lugar_ejecucion: str | None = None
    cpv_code: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class OpportunityFilters(BaseModel):
    q: str | None = None
    organismo: str | None = None
    sector: str | None = None
    tipo_contrato: str | None = None
    fecha_desde: date | None = None
    fecha_hasta: date | None = None
    importe_min: float | None = None
    importe_max: float | None = None
    sort_by: str = "fecha_publicacion"
    sort_order: str = "desc"
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=25, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page


class PaginatedSubsidies(BaseModel):
    items: list[SubsidyOut]
    total: int
    page: int
    pages: int
    per_page: int


class PaginatedTenders(BaseModel):
    items: list[TenderOut]
    total: int
    page: int
    pages: int
    per_page: int
