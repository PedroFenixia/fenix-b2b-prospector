from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    nombre_normalizado: Mapped[str] = mapped_column(Text, nullable=False)
    cif: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    forma_juridica: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    domicilio: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provincia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    localidad: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    objeto_social: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cnae_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    capital_social: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fecha_constitucion: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    fecha_primera_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    fecha_ultima_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    estado: Mapped[str] = mapped_column(Text, default="activa")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    acts: Mapped[List[Act]] = relationship(back_populates="company", cascade="all, delete-orphan")
    officers: Mapped[List[Officer]] = relationship(back_populates="company", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("nombre_normalizado", "provincia", name="uq_company_name_prov"),
        Index("idx_companies_nombre", "nombre_normalizado"),
        Index("idx_companies_provincia", "provincia"),
        Index("idx_companies_forma", "forma_juridica"),
        Index("idx_companies_estado", "estado"),
    )


class Act(Base):
    __tablename__ = "acts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    tipo_acto: Mapped[str] = mapped_column(Text, nullable=False)
    fecha_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    borme_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    borme_cve: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    datos_acto: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    texto_original: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_pdf_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped[Company] = relationship(back_populates="acts")

    __table_args__ = (
        UniqueConstraint("company_id", "borme_id", "tipo_acto", name="uq_act"),
        Index("idx_acts_company", "company_id"),
        Index("idx_acts_tipo", "tipo_acto"),
        Index("idx_acts_fecha", "fecha_publicacion"),
    )


class Officer(Base):
    __tablename__ = "officers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    nombre_persona: Mapped[str] = mapped_column(Text, nullable=False)
    cargo: Mapped[str] = mapped_column(Text, nullable=False)
    tipo_evento: Mapped[str] = mapped_column(Text, nullable=False)
    fecha_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    act_id: Mapped[Optional[int]] = mapped_column(ForeignKey("acts.id"), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped[Company] = relationship(back_populates="officers")

    __table_args__ = (
        UniqueConstraint(
            "company_id", "nombre_persona", "cargo", "tipo_evento", "fecha_publicacion",
            name="uq_officer",
        ),
        Index("idx_officers_company", "company_id"),
        Index("idx_officers_nombre", "nombre_persona"),
    )


class CnaeCode(Base):
    __tablename__ = "cnae_codes"

    code: Mapped[str] = mapped_column(Text, primary_key=True)
    description_es: Mapped[str] = mapped_column(Text, nullable=False)
    section: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    division: Mapped[Optional[str]] = mapped_column(Text, nullable=True)


class Province(Base):
    __tablename__ = "provinces"

    code: Mapped[str] = mapped_column(Text, primary_key=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    comunidad: Mapped[str] = mapped_column(Text, nullable=False)


class IngestionLog(Base):
    __tablename__ = "ingestion_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    fecha_borme: Mapped[date] = mapped_column(Date, nullable=False, unique=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    pdfs_found: Mapped[int] = mapped_column(Integer, default=0)
    pdfs_downloaded: Mapped[int] = mapped_column(Integer, default=0)
    pdfs_parsed: Mapped[int] = mapped_column(Integer, default=0)
    companies_new: Mapped[int] = mapped_column(Integer, default=0)
    companies_updated: Mapped[int] = mapped_column(Integer, default=0)
    acts_created: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )


class Subsidy(Base):
    """Subvenciones y ayudas del BOE (Sección V.B)."""
    __tablename__ = "subsidies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    boe_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    organismo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_pdf: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fecha_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    fecha_limite: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    importe: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    beneficiarios: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ambito: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_subsidies_fecha", "fecha_publicacion"),
        Index("idx_subsidies_organismo", "organismo"),
        Index("idx_subsidies_sector", "sector"),
    )


class Tender(Base):
    """Licitaciones de la Plataforma de Contratación del Sector Público."""
    __tablename__ = "tenders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    expediente: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    organismo: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    estado: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tipo_contrato: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_licitacion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fecha_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    fecha_limite: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    importe_estimado: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    lugar_ejecucion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cpv_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_tenders_fecha", "fecha_publicacion"),
        Index("idx_tenders_organismo", "organismo"),
        Index("idx_tenders_estado", "estado"),
        Index("idx_tenders_tipo", "tipo_contrato"),
    )


class JudicialNotice(Base):
    """Anuncios judiciales del BOE (concursos de acreedores, embargos, etc.)."""
    __tablename__ = "judicial_notices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    boe_id: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    juzgado: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    localidad: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provincia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    deudor: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    url_pdf: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fecha_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_judicial_fecha", "fecha_publicacion"),
        Index("idx_judicial_tipo", "tipo"),
        Index("idx_judicial_provincia", "provincia"),
        Index("idx_judicial_deudor", "deudor"),
    )


class ExportLog(Base):
    __tablename__ = "export_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(Text, nullable=False)
    filters_applied: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    record_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
