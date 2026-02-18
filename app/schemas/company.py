from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class CompanyBase(BaseModel):
    nombre: str
    cif: str | None = None
    forma_juridica: str | None = None
    domicilio: str | None = None
    provincia: str | None = None
    localidad: str | None = None
    objeto_social: str | None = None
    cnae_code: str | None = None
    capital_social: float | None = None
    fecha_constitucion: date | None = None
    email: str | None = None
    telefono: str | None = None
    web: str | None = None
    estado: str = "activa"
    datos_registrales: str | None = None
    score_solvencia: int | None = None


class CompanyOut(CompanyBase):
    id: int
    fecha_primera_publicacion: date
    fecha_ultima_publicacion: date
    created_at: datetime

    model_config = {"from_attributes": True}


class CompanyDetail(CompanyOut):
    acts: list["ActOut"] = []
    officers: list["OfficerOut"] = []


class ActOut(BaseModel):
    id: int
    tipo_acto: str
    fecha_publicacion: date
    borme_id: str | None = None
    datos_acto: str | None = None
    source_pdf_url: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}


class OfficerOut(BaseModel):
    id: int
    nombre_persona: str
    cargo: str
    tipo_evento: str
    fecha_publicacion: date

    model_config = {"from_attributes": True}


# Rebuild forward refs
CompanyDetail.model_rebuild()
