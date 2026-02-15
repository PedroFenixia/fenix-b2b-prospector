"""Backfill historical BORME data."""
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.services.ingestion_orchestrator import ingest_date_range


async def main():
    settings.ensure_dirs()

    # Default: last 30 days
    if len(sys.argv) >= 3:
        desde = date.fromisoformat(sys.argv[1])
        hasta = date.fromisoformat(sys.argv[2])
    else:
        hasta = date.today()
        desde = hasta - timedelta(days=30)

    print(f"Backfilling BORME data from {desde} to {hasta}")

    # Import here to ensure DB is created
    from app.db.engine import engine
    from app.db.models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from app.db.seed_cnae import seed_all
    await seed_all()

    await ingest_date_range(desde, hasta)
    print("Backfill complete.")


if __name__ == "__main__":
    asyncio.run(main())
