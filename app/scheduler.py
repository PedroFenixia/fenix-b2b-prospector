"""Daily scheduler for automatic data updates."""
from __future__ import annotations

import asyncio
import logging
import random
from datetime import date, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def daily_borme_update():
    """Ingest yesterday's BORME (today's may not be published yet), then enrich CIF."""
    from app.services.ingestion_orchestrator import ingest_single_date

    yesterday = date.today() - timedelta(days=1)
    logger.info(f"[Scheduler] Starting daily BORME ingestion for {yesterday}")
    try:
        await ingest_single_date(yesterday)
        logger.info(f"[Scheduler] BORME ingestion completed for {yesterday}")
    except Exception as e:
        logger.error(f"[Scheduler] BORME ingestion failed for {yesterday}: {e}")

    # Auto-enrich CIF for new companies from today's ingestion
    await _enrich_new_companies_cif(yesterday)


async def _enrich_new_companies_cif(fecha: date):
    """Enrich CIF for companies first published on a given date (new entries only)."""
    from sqlalchemy import select

    from app.db.engine import async_session
    from app.db.models import Company
    from app.services.cif_enrichment import lookup_cif_by_name

    async with async_session() as db:
        # Only companies that appeared for the first time on this date and have no CIF
        new_companies = (
            await db.scalars(
                select(Company).where(
                    Company.fecha_primera_publicacion == fecha,
                    Company.cif.is_(None),
                )
            )
        ).all()

        if not new_companies:
            logger.info(f"[CIF] No new companies without CIF for {fecha}")
            return

        logger.info(f"[CIF] Enriching {len(new_companies)} new companies from {fecha}")
        enriched = 0
        for company in new_companies:
            try:
                cif = await lookup_cif_by_name(company.nombre)
                if cif:
                    company.cif = cif
                    enriched += 1
                await asyncio.sleep(1.5)  # Rate limit
            except Exception as e:
                logger.warning(f"[CIF] Error for {company.nombre}: {e}")
                break  # Stop on errors (likely rate limit)

        await db.commit()
        logger.info(f"[CIF] Enriched {enriched}/{len(new_companies)} companies for {fecha}")


async def daily_boe_subsidies_update():
    """Fetch today's subsidies from BOE."""
    from app.db.engine import async_session
    from app.services.boe_subsidies_fetcher import fetch_boe_subsidies
    from app.services.opportunity_service import upsert_subsidies

    logger.info("[Scheduler] Fetching BOE subsidies")
    try:
        raw = await fetch_boe_subsidies(date.today())
        async with async_session() as db:
            count = await upsert_subsidies(raw, db)
        logger.info(f"[Scheduler] BOE subsidies: {len(raw)} fetched, {count} new")
    except Exception as e:
        logger.error(f"[Scheduler] BOE subsidies failed: {e}")


async def daily_placsp_tenders_update():
    """Fetch recent tenders from PLACSP."""
    from app.db.engine import async_session
    from app.services.opportunity_service import upsert_tenders
    from app.services.placsp_fetcher import fetch_recent_tenders

    logger.info("[Scheduler] Fetching PLACSP tenders")
    try:
        raw = await fetch_recent_tenders(max_entries=100)
        async with async_session() as db:
            count = await upsert_tenders(raw, db)
        logger.info(f"[Scheduler] PLACSP tenders: {len(raw)} fetched, {count} new")
    except Exception as e:
        logger.error(f"[Scheduler] PLACSP tenders failed: {e}")


async def daily_boe_judicial_update():
    """Fetch today's judicial notices from BOE."""
    from app.db.engine import async_session
    from app.services.boe_judicial_fetcher import fetch_boe_judicial
    from app.services.opportunity_service import upsert_judicial

    logger.info("[Scheduler] Fetching BOE judicial notices")
    try:
        raw = await fetch_boe_judicial(date.today())
        async with async_session() as db:
            count = await upsert_judicial(raw, db)
        logger.info(f"[Scheduler] BOE judicial: {len(raw)} fetched, {count} new")
    except Exception as e:
        logger.error(f"[Scheduler] BOE judicial failed: {e}")


async def daily_archive_expired():
    """Archive subsidies and tenders whose deadline has passed."""
    from app.db.engine import async_session
    from app.services.opportunity_service import archive_expired

    logger.info("[Scheduler] Archiving expired opportunities")
    try:
        async with async_session() as db:
            stats = await archive_expired(db)
        logger.info(f"[Scheduler] Archive: {stats['subsidies_archived']} subsidies, {stats['tenders_archived']} tenders")
    except Exception as e:
        logger.error(f"[Scheduler] Archive failed: {e}")


# --- CIF enrichment state ---
_cif_running = False
_cif_stop = False
_cif_stats = {
    "total": 0, "attempted": 0, "found": 0,
    "errors": 0, "current_company": "",
}

# --- Web/contact enrichment state ---
_web_running = False
_web_stop = False
_web_stats = {
    "total": 0, "attempted": 0, "found": 0,
    "errors": 0, "current_company": "",
}


def get_cif_enrichment_stats() -> dict:
    return {"running": _cif_running, **dict(_cif_stats)}


def get_web_enrichment_stats() -> dict:
    return {"running": _web_running, **dict(_web_stats)}


def get_enrichment_stats() -> dict:
    """Combined stats for backward compatibility."""
    return {
        "running": _cif_running or _web_running,
        "phase": "cif" if _cif_running else ("web" if _web_running else _cif_stats.get("_last_phase", "")),
        "cif_total": _cif_stats["total"], "cif_attempted": _cif_stats["attempted"], "cif_found": _cif_stats["found"],
        "web_total": _web_stats["total"], "web_attempted": _web_stats["attempted"], "web_found": _web_stats["found"],
        "errors": _cif_stats["errors"] + _web_stats["errors"],
        "current_company": _cif_stats["current_company"] if _cif_running else _web_stats["current_company"],
    }


def stop_cif_enrichment():
    global _cif_stop
    _cif_stop = True


def stop_web_enrichment():
    global _web_stop
    _web_stop = True


def stop_enrichment():
    """Stop whichever enrichment is running."""
    stop_cif_enrichment()
    stop_web_enrichment()


async def enrichment_cif():
    """Batch CIF enrichment for all companies without CIF."""
    global _cif_running, _cif_stop
    if _cif_running:
        logger.info("[CIF Batch] Already running, skipping")
        return
    _cif_running = True
    _cif_stop = False

    from sqlalchemy import select, func as f
    from app.db.engine import async_session
    from app.db.models import Company
    from app.services.cif_enrichment import lookup_cif_by_name

    stats = _cif_stats
    stats.update({"total": 0, "attempted": 0, "found": 0, "errors": 0, "current_company": ""})

    try:
        async with async_session() as db:
            total = await db.scalar(select(f.count(Company.id)).where(Company.cif.is_(None))) or 0
            stats["total"] = total
            logger.info(f"[CIF Batch] {total} companies without CIF")
            offset = 0
            while not _cif_stop:
                companies = (await db.scalars(
                    select(Company).where(Company.cif.is_(None))
                    .order_by(Company.fecha_ultima_publicacion.desc())
                    .offset(offset).limit(100)
                )).all()
                if not companies:
                    break
                for c in companies:
                    if _cif_stop:
                        break
                    stats["attempted"] += 1
                    stats["current_company"] = c.nombre[:60]
                    try:
                        cif = await lookup_cif_by_name(c.nombre, use_google=False)
                        if cif:
                            c.cif = cif
                            stats["found"] += 1
                        await asyncio.sleep(random.uniform(3, 8))
                    except Exception as e:
                        stats["errors"] += 1
                        logger.warning(f"[CIF Batch] Error {c.nombre}: {e}")
                        await asyncio.sleep(random.uniform(10, 30))
                await db.commit()
                offset += 100
                logger.info(f"[CIF Batch] {stats['attempted']}/{total}, found: {stats['found']}")

        logger.info(f"[CIF Batch] {'Stopped' if _cif_stop else 'Completed'}: {stats}")
    except Exception as e:
        logger.error(f"[CIF Batch] Fatal error: {e}")
    finally:
        stats["current_company"] = ""
        stats["_last_phase"] = "done" if not _cif_stop else "stopped"
        _cif_running = False
        _cif_stop = False
    return stats


async def enrichment_web():
    """Batch web/contact enrichment for all active companies without web."""
    global _web_running, _web_stop
    if _web_running:
        logger.info("[Web Batch] Already running, skipping")
        return
    _web_running = True
    _web_stop = False

    from sqlalchemy import select, func as f
    from app.db.engine import async_session
    from app.db.models import Company
    from app.services.web_enrichment import enrich_company_web
    import httpx

    stats = _web_stats
    stats.update({"total": 0, "attempted": 0, "found": 0, "errors": 0, "current_company": ""})

    try:
        async with async_session() as db:
            total = await db.scalar(select(f.count(Company.id)).where(Company.web.is_(None), Company.estado == "activa")) or 0
            stats["total"] = total
            logger.info(f"[Web Batch] {total} active companies without web")
            offset = 0
            async with httpx.AsyncClient(timeout=15.0) as client:
                while not _web_stop:
                    companies = (await db.scalars(
                        select(Company).where(Company.web.is_(None), Company.estado == "activa")
                        .order_by(Company.fecha_ultima_publicacion.desc())
                        .offset(offset).limit(100)
                    )).all()
                    if not companies:
                        break
                    for c in companies:
                        if _web_stop:
                            break
                        stats["attempted"] += 1
                        stats["current_company"] = c.nombre[:60]
                        try:
                            r = await enrich_company_web(c, client)
                            if r["web"]:
                                c.web = r["web"]
                                stats["found"] += 1
                            if r["email"]:
                                c.email = r["email"]
                            if r["telefono"]:
                                c.telefono = r["telefono"]
                            await asyncio.sleep(random.uniform(4, 10))
                        except Exception as e:
                            stats["errors"] += 1
                            logger.warning(f"[Web Batch] Error {c.nombre}: {e}")
                            await asyncio.sleep(random.uniform(10, 30))
                    await db.commit()
                    offset += 100
                    logger.info(f"[Web Batch] {stats['attempted']}/{total}, found: {stats['found']}")

        logger.info(f"[Web Batch] {'Stopped' if _web_stop else 'Completed'}: {stats}")
    except Exception as e:
        logger.error(f"[Web Batch] Fatal error: {e}")
    finally:
        stats["current_company"] = ""
        stats["_last_phase"] = "done" if not _web_stop else "stopped"
        _web_running = False
        _web_stop = False
    return stats


def is_enrichment_running() -> bool:
    return _cif_running or _web_running


def is_cif_running() -> bool:
    return _cif_running


def is_web_running() -> bool:
    return _web_running


def start_scheduler(hour: int = 10, minute: int = 0):
    """Start the daily scheduler. Runs all updates at the configured time."""
    # BORME at configured hour
    scheduler.add_job(
        daily_borme_update,
        CronTrigger(hour=hour, minute=minute),
        id="daily_borme",
        replace_existing=True,
    )

    # BOE subsidies 15 min later
    scheduler.add_job(
        daily_boe_subsidies_update,
        CronTrigger(hour=hour, minute=minute + 15),
        id="daily_subsidies",
        replace_existing=True,
    )

    # PLACSP tenders 30 min later
    scheduler.add_job(
        daily_placsp_tenders_update,
        CronTrigger(hour=hour, minute=minute + 30),
        id="daily_tenders",
        replace_existing=True,
    )

    # BOE judicial 45 min later
    scheduler.add_job(
        daily_boe_judicial_update,
        CronTrigger(hour=hour, minute=minute + 45),
        id="daily_judicial",
        replace_existing=True,
    )

    # Archive expired opportunities at midnight
    scheduler.add_job(
        daily_archive_expired,
        CronTrigger(hour=0, minute=5),
        id="daily_archive",
        replace_existing=True,
    )

    scheduler.start()
    logger.info(
        f"[Scheduler] Started - daily updates at {hour:02d}:{minute:02d}, "
        f"{hour:02d}:{minute+15:02d}, {hour:02d}:{minute+30:02d}, {hour:02d}:{minute+45:02d}, "
        f"archive at 00:05"
    )


def stop_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("[Scheduler] Stopped")
