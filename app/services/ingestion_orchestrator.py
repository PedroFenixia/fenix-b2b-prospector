"""Orchestrate the full BORME ingestion pipeline."""
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from typing import Optional

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import async_session
from app.db.models import Act, Company, IngestionLog, Officer
from app.services.borme_fetcher import BormePdfEntry, fetch_sumario
from app.services.borme_parser import ParsedCompany, parse_borme_pdf
from app.services.data_normalizer import normalize_company
from app.services.pdf_downloader import download_pdfs
from app.utils.text_clean import normalize_name

logger = logging.getLogger(__name__)

# Global state for tracking current ingestion
_current_ingestion: Optional[dict] = None

# Concurrency settings
DATE_BATCH_SIZE = 5  # Process 5 dates in parallel
PDF_PARSE_WORKERS = 6  # Parse 6 PDFs in parallel


def get_ingestion_status() -> dict:
    return _current_ingestion or {"is_running": False}


async def ingest_date_range(fecha_desde: date, fecha_hasta: date):
    """Ingest BORME data for a range of dates, processing in parallel batches."""
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

    # Build list of all dates
    all_dates = [fecha_desde + timedelta(days=i) for i in range(total_days)]

    try:
        # Process in batches of DATE_BATCH_SIZE
        for i in range(0, len(all_dates), DATE_BATCH_SIZE):
            batch = all_dates[i : i + DATE_BATCH_SIZE]
            _current_ingestion["current_date"] = f"{batch[0]} .. {batch[-1]}"

            # Run batch in parallel
            results = await asyncio.gather(
                *[ingest_single_date(d) for d in batch],
                return_exceptions=True,
            )

            # Log any errors but don't stop
            for d, r in zip(batch, results):
                if isinstance(r, Exception):
                    logger.error(f"Batch ingestion failed for {d}: {r}")

            _current_ingestion["processed"] += len(batch)
    finally:
        _current_ingestion = {"is_running": False}


async def ingest_single_date(fecha: date):
    """Full pipeline for one date."""
    async with async_session() as db:
        # Check if already processed
        existing = await db.scalar(
            select(IngestionLog.id).where(
                IngestionLog.fecha_borme == fecha,
                IngestionLog.status == "completed",
            )
        )
        if existing:
            logger.info(f"Date {fecha} already ingested, skipping.")
            return

        # Create or update ingestion log
        log = IngestionLog(
            fecha_borme=fecha,
            status="fetching",
            started_at=datetime.utcnow(),
        )
        db.add(log)
        await db.commit()
        await db.refresh(log)

        try:
            # Step 1: Fetch sumario
            sumario = await fetch_sumario(fecha)
            if sumario is None or not sumario.pdfs:
                log.status = "completed"
                log.completed_at = datetime.utcnow()
                log.error_message = "No BORME published this date"
                await db.commit()
                return

            log.pdfs_found = len(sumario.pdfs)
            log.status = "downloading"
            await db.commit()

            # Step 2: Download PDFs
            fecha_str = fecha.strftime("%Y%m%d")
            downloaded = await download_pdfs(sumario.pdfs, fecha_str)
            log.pdfs_downloaded = len(downloaded)
            log.status = "parsing"
            await db.commit()

            # Step 3: Parse PDFs in parallel (CPU-bound, use thread pool)
            companies_new = 0
            companies_updated = 0
            acts_created = 0
            pdfs_parsed = 0

            loop = asyncio.get_running_loop()
            parse_sem = asyncio.Semaphore(PDF_PARSE_WORKERS)

            async def parse_one(entry_path_pair):
                entry, pdf_path = entry_path_pair
                async with parse_sem:
                    parsed = await loop.run_in_executor(
                        None, parse_borme_pdf, pdf_path
                    )
                return entry, parsed

            parse_tasks = [parse_one(ep) for ep in downloaded]
            parse_results = await asyncio.gather(*parse_tasks, return_exceptions=True)

            for pr in parse_results:
                if isinstance(pr, Exception):
                    logger.error(f"PDF parse error: {pr}")
                    continue
                entry, parsed_companies = pr
                if parsed_companies:
                    pdfs_parsed += 1

                for parsed in parsed_companies:
                    result = await _store_company(db, parsed, entry, fecha)
                    companies_new += result["new"]
                    companies_updated += result["updated"]
                    acts_created += result["acts"]

            log.pdfs_parsed = pdfs_parsed
            log.companies_new = companies_new
            log.companies_updated = companies_updated
            log.acts_created = acts_created
            log.status = "completed"
            log.completed_at = datetime.utcnow()
            await db.commit()

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
            raise


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
        # Always update ultima publicacion and estado
        existing.fecha_ultima_publicacion = fecha
        existing.estado = normalized["estado"]
        company = existing
        result["updated"] = 1
    else:
        # Create new company
        company = Company(**normalized)
        db.add(company)
        await db.flush()
        result["new"] = 1

    # Store acts
    for parsed_act in parsed.actos:
        # Check for duplicate
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
