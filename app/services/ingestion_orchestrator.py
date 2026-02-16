"""Orchestrate the full BORME ingestion pipeline.

Optimized for speed: parallel downloads + parsing, sequential DB writes.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import async_session
from app.db.models import Act, Company, IngestionLog, Officer
from app.services.borme_fetcher import BormePdfEntry, fetch_sumario
from app.services.borme_parser import ParsedCompany, parse_borme_pdf
from app.services.data_normalizer import normalize_company
from app.services.pdf_downloader import download_pdfs

logger = logging.getLogger(__name__)

# Global state for tracking current ingestion
_current_ingestion: Optional[dict] = None

# Moderate concurrency - safe for BOE without triggering blocks
PREFETCH_AHEAD = 3
PDF_PARSE_WORKERS = 4
PAUSE_BETWEEN_DATES = 0.5  # seconds between dates


def get_ingestion_status() -> dict:
    return _current_ingestion or {"is_running": False}


async def _fetch_and_parse(fecha: date):
    """Fetch sumario, download PDFs, and parse them. Returns parsed data (no DB writes)."""
    sumario = await fetch_sumario(fecha)
    if sumario is None or not sumario.pdfs:
        return fecha, None, []

    fecha_str = fecha.strftime("%Y%m%d")
    downloaded = await download_pdfs(sumario.pdfs, fecha_str)

    # Parse PDFs in parallel using thread pool (CPU-bound)
    loop = asyncio.get_running_loop()
    sem = asyncio.Semaphore(PDF_PARSE_WORKERS)

    async def parse_one(entry, pdf_path):
        async with sem:
            parsed = await loop.run_in_executor(None, parse_borme_pdf, pdf_path)
        return entry, parsed

    parse_results = await asyncio.gather(
        *[parse_one(e, p) for e, p in downloaded],
        return_exceptions=True,
    )

    all_parsed = []
    for pr in parse_results:
        if isinstance(pr, Exception):
            logger.error(f"PDF parse error for {fecha}: {pr}")
            continue
        entry, companies = pr
        for c in companies:
            all_parsed.append((entry, c))

    return fecha, sumario, all_parsed


async def ingest_date_range(fecha_desde: date, fecha_hasta: date, reverse: bool = True):
    """Ingest BORME data for a range of dates with prefetch pipeline.

    Args:
        reverse: If True, process from most recent to oldest (default).
    """
    global _current_ingestion
    total_days = (fecha_hasta - fecha_desde).days + 1
    _current_ingestion = {
        "is_running": True,
        "fecha_desde": str(fecha_desde),
        "fecha_hasta": str(fecha_hasta),
        "current_date": None,
        "processed": 0,
        "total": total_days,
    }

    all_dates = [fecha_desde + timedelta(days=i) for i in range(total_days)]
    if reverse:
        all_dates.reverse()

    try:
        # Process in batches: prefetch+parse in parallel, then write to DB sequentially
        for i in range(0, len(all_dates), PREFETCH_AHEAD):
            batch = all_dates[i : i + PREFETCH_AHEAD]
            _current_ingestion["current_date"] = f"{batch[0]} .. {batch[-1]}"

            # Filter out already-completed dates before fetching
            dates_to_fetch = []
            async with async_session() as db:
                for d in batch:
                    existing = await db.scalar(
                        select(IngestionLog.status).where(
                            IngestionLog.fecha_borme == d
                        )
                    )
                    if existing == "completed":
                        logger.info(f"Date {d} already ingested, skipping.")
                    else:
                        dates_to_fetch.append(d)

            if not dates_to_fetch:
                _current_ingestion["processed"] += len(batch)
                continue

            # Prefetch + parse all dates in parallel (network + CPU, no DB)
            fetch_results = await asyncio.gather(
                *[_fetch_and_parse(d) for d in dates_to_fetch],
                return_exceptions=True,
            )

            # Now write to DB sequentially (SQLite safe)
            for fr in fetch_results:
                if isinstance(fr, Exception):
                    logger.error(f"Fetch/parse batch error: {fr}")
                    continue

                fecha, sumario, all_parsed = fr
                await _store_date_results(fecha, sumario, all_parsed)

            _current_ingestion["processed"] += len(batch)

            # Polite pause between date batches to avoid rate limiting
            if PAUSE_BETWEEN_DATES > 0:
                await asyncio.sleep(PAUSE_BETWEEN_DATES)
    finally:
        _current_ingestion = {"is_running": False}


async def _store_date_results(fecha: date, sumario, all_parsed: list):
    """Write all results for one date to DB (sequential, SQLite-safe)."""
    async with async_session() as db:
        # Get or create ingestion log
        log = await db.scalar(
            select(IngestionLog).where(IngestionLog.fecha_borme == fecha)
        )
        if log and log.status == "completed":
            return
        if log:
            log.status = "storing"
            log.started_at = datetime.utcnow()
            log.error_message = None
        else:
            log = IngestionLog(
                fecha_borme=fecha,
                status="storing",
                started_at=datetime.utcnow(),
            )
            db.add(log)
        await db.commit()
        await db.refresh(log)

        try:
            if sumario is None:
                log.status = "completed"
                log.completed_at = datetime.utcnow()
                log.error_message = "No BORME published this date"
                await db.commit()
                return

            log.pdfs_found = len(sumario.pdfs) if sumario.pdfs else 0
            companies_new = 0
            companies_updated = 0
            acts_created = 0

            for entry, parsed in all_parsed:
                result = await _store_company(db, parsed, entry, fecha)
                companies_new += result["new"]
                companies_updated += result["updated"]
                acts_created += result["acts"]

            log.companies_new = companies_new
            log.companies_updated = companies_updated
            log.acts_created = acts_created
            log.status = "completed"
            log.completed_at = datetime.utcnow()
            await db.commit()

            # Generate alerts for watched companies
            try:
                from app.services.watchlist_service import generate_alerts_for_date
                alerts_count = await generate_alerts_for_date(fecha, db)
                if alerts_count:
                    logger.info(f"Generated {alerts_count} alerts for {fecha}")
            except Exception as e:
                logger.warning(f"Alert generation failed for {fecha}: {e}")

            logger.info(
                f"Ingested {fecha}: {companies_new} new, "
                f"{companies_updated} updated, {acts_created} acts"
            )

        except Exception as e:
            log.status = "failed"
            log.error_message = str(e)[:500]
            log.completed_at = datetime.utcnow()
            await db.commit()
            logger.error(f"Ingestion failed for {fecha}: {e}")


async def ingest_single_date(fecha: date):
    """Full pipeline for one date (used by scheduler)."""
    fecha_data, sumario, all_parsed = await _fetch_and_parse(fecha)
    await _store_date_results(fecha, sumario, all_parsed)


async def _store_company(
    db: AsyncSession,
    parsed: ParsedCompany,
    entry: BormePdfEntry,
    fecha: date,
) -> dict:
    """Store a single parsed company and its acts. Returns count of new/updated/acts."""
    result = {"new": 0, "updated": 0, "acts": 0}
    normalized = normalize_company(parsed, entry.provincia, fecha)

    # Find existing company by normalized name + province
    existing = await db.scalar(
        select(Company).where(
            Company.nombre_normalizado == normalized["nombre_normalizado"],
            Company.provincia == normalized["provincia"],
        )
    )

    if existing:
        # Update existing company
        if normalized["objeto_social"] and not existing.objeto_social:
            existing.objeto_social = normalized["objeto_social"]
        if normalized["domicilio"] and not existing.domicilio:
            existing.domicilio = normalized["domicilio"]
        if normalized["capital_social"] and (not existing.capital_social or normalized["capital_social"] > existing.capital_social):
            existing.capital_social = normalized["capital_social"]
        if normalized["cnae_code"] and not existing.cnae_code:
            existing.cnae_code = normalized["cnae_code"]
        if normalized["fecha_constitucion"] and not existing.fecha_constitucion:
            existing.fecha_constitucion = normalized["fecha_constitucion"]
        if normalized["localidad"] and not existing.localidad:
            existing.localidad = normalized["localidad"]
        existing.fecha_ultima_publicacion = fecha
        existing.estado = normalized["estado"]
        company = existing
        result["updated"] = 1
    else:
        company = Company(**normalized)
        db.add(company)
        await db.flush()
        result["new"] = 1

    # Store acts
    for parsed_act in parsed.actos:
        existing_act = await db.scalar(
            select(Act.id).where(
                Act.company_id == company.id,
                Act.borme_id == entry.id,
                Act.tipo_acto == parsed_act.tipo,
            )
        )
        if existing_act:
            continue

        act = Act(
            company_id=company.id,
            tipo_acto=parsed_act.tipo,
            fecha_publicacion=fecha,
            borme_id=entry.id,
            datos_acto=json.dumps(
                {"officers": [{"nombre": o.nombre, "cargo": o.cargo} for o in parsed_act.officers]},
                ensure_ascii=False,
            ) if parsed_act.officers else None,
            texto_original=parsed_act.texto[:2000] if parsed_act.texto else None,
            source_pdf_url=entry.url_pdf,
        )
        db.add(act)
        await db.flush()
        result["acts"] += 1

        # Store officers
        for officer in parsed_act.officers:
            tipo_evento = "nombramiento"
            if parsed_act.tipo == "Ceses/Dimisiones":
                tipo_evento = "cese"
            elif parsed_act.tipo == "Revocaciones":
                tipo_evento = "revocacion"
            elif parsed_act.tipo == "Reelecciones":
                tipo_evento = "reeleccion"

            existing_officer = await db.scalar(
                select(Officer.id).where(
                    Officer.company_id == company.id,
                    Officer.nombre_persona == officer.nombre,
                    Officer.cargo == officer.cargo,
                    Officer.tipo_evento == tipo_evento,
                    Officer.fecha_publicacion == fecha,
                )
            )
            if not existing_officer:
                db.add(Officer(
                    company_id=company.id,
                    nombre_persona=officer.nombre,
                    cargo=officer.cargo,
                    tipo_evento=tipo_evento,
                    fecha_publicacion=fecha,
                    act_id=act.id,
                ))

    await db.commit()
    return result
