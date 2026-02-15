from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class ActOut(BaseModel):
    id: int
    company_id: int
    tipo_acto: str
    fecha_publicacion: date
    borme_id: str | None = None
    borme_cve: str | None = None
    datos_acto: str | None = None
    source_pdf_url: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
