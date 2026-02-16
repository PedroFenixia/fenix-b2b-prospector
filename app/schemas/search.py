from __future__ import annotations

from datetime import date

from pydantic import BaseModel, Field


class SearchFilters(BaseModel):
    q: str | None = None
    cif: str | None = None
    provincia: str | None = None
    forma_juridica: str | None = None
    cnae_code: str | None = None
    tipo_acto: str | None = None
    estado: str | None = None
    fecha_desde: date | None = None
    fecha_hasta: date | None = None
    pub_desde: date | None = None
    pub_hasta: date | None = None
    capital_min: float | None = None
    capital_max: float | None = None
    score_min: int | None = None
    sort_by: str = "fecha_ultima_publicacion"
    sort_order: str = "desc"
    page: int = Field(default=1, ge=1)
    per_page: int = Field(default=25, ge=1, le=100)

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.per_page


class PaginatedResponse(BaseModel):
    items: list
    total: int
    page: int
    pages: int
    per_page: int
