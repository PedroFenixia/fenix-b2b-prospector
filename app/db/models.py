from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

from sqlalchemy import (
    Boolean,
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


class User(Base):
    """Usuarios del sistema."""
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    empresa: Mapped[str] = mapped_column(Text, nullable=False, default="")
    empresa_cif: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    telefono: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    email_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    phone_verified: Mapped[bool] = mapped_column(Boolean, default=False)
    verification_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    verification_code_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, default="user")  # admin, user
    plan: Mapped[str] = mapped_column(Text, default="free")  # free, pro, enterprise
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    searches_this_month: Mapped[int] = mapped_column(Integer, default=0)
    exports_this_month: Mapped[int] = mapped_column(Integer, default=0)
    detail_views_this_month: Mapped[int] = mapped_column(Integer, default=0)
    enrichments_this_month: Mapped[int] = mapped_column(Integer, default=0)
    month_reset: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # YYYY-MM
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index("idx_users_email", "email"),
        Index("idx_users_plan", "plan"),
    )


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
    cnae_inferred: Mapped[bool] = mapped_column(Boolean, default=False)
    capital_social: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    fecha_constitucion: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    fecha_primera_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    fecha_ultima_publicacion: Mapped[date] = mapped_column(Date, nullable=False)
    email: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    telefono: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    web: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    estado: Mapped[str] = mapped_column(Text, default="activa")
    num_empleados: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    facturacion: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    datos_registrales: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    cif_intentos: Mapped[int] = mapped_column(Integer, default=0)
    web_intentos: Mapped[int] = mapped_column(Integer, default=0)
    score_solvencia: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    score_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
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
        Index("idx_companies_cif", "cif"),
        Index("idx_companies_fecha_pub", "fecha_ultima_publicacion"),
        Index("idx_companies_score", "score_solvencia"),
        Index("idx_companies_cnae", "cnae_code"),
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
    cnae_codes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    comunidad_autonoma: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provincia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    archivada: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_subsidies_fecha", "fecha_publicacion"),
        Index("idx_subsidies_organismo", "organismo"),
        Index("idx_subsidies_sector", "sector"),
        Index("idx_subsidies_ccaa", "comunidad_autonoma"),
        Index("idx_subsidies_archivada", "archivada"),
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
    cnae_codes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    comunidad_autonoma: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    provincia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    archivada: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_tenders_fecha", "fecha_publicacion"),
        Index("idx_tenders_organismo", "organismo"),
        Index("idx_tenders_estado", "estado"),
        Index("idx_tenders_tipo", "tipo_contrato"),
        Index("idx_tenders_ccaa", "comunidad_autonoma"),
        Index("idx_tenders_archivada", "archivada"),
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
    deudor_cif: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
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


class Watchlist(Base):
    """Empresas bajo vigilancia."""
    __tablename__ = "watchlist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    tipos_acto: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON list or null=all
    notas: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped[Company] = relationship()

    __table_args__ = (
        Index("idx_watchlist_company", "company_id"),
        Index("idx_watchlist_user", "user_id"),
    )


class ActTypeWatch(Base):
    """Suscripciones globales a tipos de acto (ej: Constitución, Situación concursal)."""
    __tablename__ = "act_type_watches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    tipo_acto: Mapped[str] = mapped_column(Text, nullable=False)
    filtro_provincia: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        UniqueConstraint("user_id", "tipo_acto", "filtro_provincia", name="uq_act_type_watch"),
        Index("idx_act_type_watch_user", "user_id"),
    )


class Alert(Base):
    """Alertas generadas cuando una empresa vigilada tiene actividad nueva."""
    __tablename__ = "alerts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=True)
    company_id: Mapped[int] = mapped_column(ForeignKey("companies.id", ondelete="CASCADE"), nullable=False)
    act_id: Mapped[Optional[int]] = mapped_column(ForeignKey("acts.id", ondelete="SET NULL"), nullable=True)
    tipo: Mapped[str] = mapped_column(Text, nullable=False)
    titulo: Mapped[str] = mapped_column(Text, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source: Mapped[str] = mapped_column(Text, default="watchlist")  # watchlist, act_type
    leida: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    company: Mapped[Company] = relationship()
    act: Mapped[Optional[Act]] = relationship()

    __table_args__ = (
        Index("idx_alerts_company", "company_id"),
        Index("idx_alerts_user", "user_id"),
        Index("idx_alerts_leida", "leida"),
        Index("idx_alerts_created", "created_at"),
    )


class ApiKey(Base):
    """Claves de API para integracion ERP."""
    __tablename__ = "api_keys"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[str] = mapped_column(Text, nullable=False, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )

    __table_args__ = (
        Index("idx_api_keys_key", "key"),
        Index("idx_api_keys_user", "user_id"),
    )


class ExportLog(Base):
    __tablename__ = "export_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    filename: Mapped[str] = mapped_column(Text, nullable=False)
    format: Mapped[str] = mapped_column(Text, nullable=False)
    filters_applied: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    record_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now()
    )


class ERPConnection(Base):
    """Conexión ERP configurada por el usuario (Odoo, SAP, Holded…)."""
    __tablename__ = "erp_connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)  # odoo, sap, holded, webhook
    name: Mapped[str] = mapped_column(Text, nullable=False, default="Mi ERP")
    url: Mapped[str] = mapped_column(Text, nullable=False)  # Base URL del ERP
    database: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # DB name (Odoo)
    username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    api_key: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # API key o password
    field_mapping: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # JSON field mapping
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_sync_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    last_sync_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # ok, error
    last_sync_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_erp_conn_user", "user_id"),
    )


class ERPSyncLog(Base):
    """Historial de sincronizaciones con ERP."""
    __tablename__ = "erp_sync_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    connection_id: Mapped[int] = mapped_column(ForeignKey("erp_connections.id", ondelete="CASCADE"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    action: Mapped[str] = mapped_column(Text, nullable=False)  # push_companies, push_leads
    companies_sent: Mapped[int] = mapped_column(Integer, default=0)
    companies_created: Mapped[int] = mapped_column(Integer, default=0)
    companies_updated: Mapped[int] = mapped_column(Integer, default=0)
    companies_failed: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")  # pending, running, ok, error
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    __table_args__ = (
        Index("idx_erp_sync_conn", "connection_id"),
        Index("idx_erp_sync_user", "user_id"),
    )
