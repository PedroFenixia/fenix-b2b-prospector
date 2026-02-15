from __future__ import annotations

from datetime import date, datetime

from pydantic import BaseModel


class IngestionTrigger(BaseModel):
    fecha_desde: date
    fecha_hasta: date


class IngestionLogOut(BaseModel):
    id: int
    fecha_borme: date
    status: str
    pdfs_found: int
    pdfs_downloaded: int
    pdfs_parsed: int
    companies_new: int
    companies_updated: int
    acts_created: int
    error_message: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None

    model_config = {"from_attributes": True}


class IngestionStatus(BaseModel):
    is_running: bool
    current_date: str | None = None
    recent_jobs: list[IngestionLogOut] = []
