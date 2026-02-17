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


_enrichment_running = False
_enrichment_stop = False
_enrichment_stats = {
    "phase": "",
    "cif_total": 0, "cif_attempted": 0, "cif_found": 0,
    "web_total": 0, "web_attempted": 0, "web_found": 0,
    "errors": 0, "current_company": "",
}


def get_enrichment_stats() -> dict:
    """Get current enrichment stats for live progress."""
    return dict(_enrichment_stats)


def stop_enrichment():
    """Signal the enrichment process to stop."""
    global _enrichment_stop
    _enrichment_stop = True


async def full_enrichment():
    """Run full CIF + web enrichment for ALL companies missing data."""
    global _enrichment_running, _enrichment_stop
    if _enrichment_running:
        logger.info("[Enrichment] Already running, skipping")
        return
    _enrichment_running = True
    _enrichment_stop = False

    from sqlalchemy import select, func as f
    from app.db.engine import async_session
    from app.db.models import Company
    from app.services.cif_enrichment import lookup_cif_by_name
    from app.services.web_enrichment import enrich_company_web
    import httpx

    stats = _enrichment_stats
    stats.update({"phase": "cif", "cif_total": 0, "cif_attempted": 0, "cif_found": 0,
                  "web_total": 0, "web_attempted": 0, "web_found": 0, "errors": 0, "current_company": ""})

    try:
        # Phase 1: CIF enrichment
        async with async_session() as db:
            total = await db.scalar(select(f.count(Company.id)).where(Company.cif.is_(None))) or 0
            stats["cif_total"] = total
            logger.info(f"[Enrichment] Phase 1: {total} companies without CIF")
            offset = 0
            while not _enrichment_stop:
                companies = (await db.scalars(
                    select(Company).where(Company.cif.is_(None))
                    .order_by(Company.fecha_ultima_publicacion.desc())
                    .offset(offset).limit(100)
                )).all()
                if not companies:
                    break
                for c in companies:
                    if _enrichment_stop:
                        break
                    stats["cif_attempted"] += 1
                    stats["current_company"] = c.nombre[:60]
                    try:
                        cif = await lookup_cif_by_name(c.nombre, use_google=False)
                        if cif:
                            c.cif = cif
                            stats["cif_found"] += 1
                        await asyncio.sleep(random.uniform(3, 8))
                    except Exception as e:
                        stats["errors"] += 1
                        logger.warning(f"[Enrichment] CIF error {c.nombre}: {e}")
                        await asyncio.sleep(random.uniform(10, 30))
                await db.commit()
                offset += 100
                logger.info(f"[Enrichment] CIF: {stats['cif_attempted']}/{total}, found: {stats['cif_found']}")

        # Phase 2: Web enrichment
        if not _enrichment_stop:
            async with async_session() as db:
                total = await db.scalar(select(f.count(Company.id)).where(Company.web.is_(None), Company.estado == "activa")) or 0
                stats["web_total"] = total
                stats["phase"] = "web"
                logger.info(f"[Enrichment] Phase 2: {total} companies without web")
                offset = 0
                async with httpx.AsyncClient(timeout=15.0) as client:
                    while not _enrichment_stop:
                        companies = (await db.scalars(
                            select(Company).where(Company.web.is_(None), Company.estado == "activa")
                            .order_by(Company.fecha_ultima_publicacion.desc())
                            .offset(offset).limit(100)
                        )).all()
                        if not companies:
                            break
                        for c in companies:
                            if _enrichment_stop:
                                break
                            stats["web_attempted"] += 1
                            stats["current_company"] = c.nombre[:60]
                            try:
                                r = await enrich_company_web(c, client)
                                if r["web"]:
                                    c.web = r["web"]
                                    stats["web_found"] += 1
                                if r["cif"] and not c.cif:
                                    c.cif = r["cif"]
                                if r["email"]:
                                    c.email = r["email"]
                                if r["telefono"]:
                                    c.telefono = r["telefono"]
                                await asyncio.sleep(random.uniform(4, 10))
                            except Exception as e:
                                stats["errors"] += 1
                                logger.warning(f"[Enrichment] Web error {c.nombre}: {e}")
                                await asyncio.sleep(random.uniform(10, 30))
                        await db.commit()
                        offset += 100
                        logger.info(f"[Enrichment] Web: {stats['web_attempted']}/{total}, found: {stats['web_found']}")

        if _enrichment_stop:
            logger.info(f"[Enrichment] Stopped by user: {stats}")
        else:
            logger.info(f"[Enrichment] Completed: {stats}")
    except Exception as e:
        logger.error(f"[Enrichment] Fatal error: {e}")
    finally:
        stats["phase"] = "done" if not _enrichment_stop else "stopped"
        stats["current_company"] = ""
        _enrichment_running = False
        _enrichment_stop = False
    return stats


def is_enrichment_running() -> bool:
    return _enrichment_running


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

    scheduler.start()
    logger.info(
        f"[Scheduler] Started - daily updates at {hour:02d}:{minute:02d}, "
        f"{hour:02d}:{minute+15:02d}, {hour:02d}:{minute+30:02d}, {hour:02d}:{minute+45:02d}"
    )


def stop_scheduler():
    """Stop the scheduler."""
    if scheduler.running:
        scheduler.shutdown()
        logger.info("[Scheduler] Stopped")
