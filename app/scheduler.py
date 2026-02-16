"""Daily scheduler for automatic data updates."""
from __future__ import annotations

import asyncio
import logging
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
    from app.config import settings

    if not settings.apiempresas_key:
        return

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
                cif = await lookup_cif_by_name(company.nombre, settings.apiempresas_key)
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
