"""Sincroniza empresas de SQLite a Typesense.

Uso:
  python scripts/sync_typesense.py                      # Full sync
  python scripts/sync_typesense.py --since 2025-01-01   # Incremental
  python scripts/sync_typesense.py --recreate            # Drop + recrear coleccion
  python scripts/sync_typesense.py --synonyms            # Solo sincronizar sinonimos
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, select

from app.config import settings
from app.db.models import Company


async def main() -> None:
    parser = argparse.ArgumentParser(description="Sync SQLite → Typesense")
    parser.add_argument("--since", type=str, default=None,
                        help="Sync incremental: solo empresas actualizadas desde esta fecha (YYYY-MM-DD)")
    parser.add_argument("--recreate", action="store_true",
                        help="Eliminar y recrear la coleccion antes de sincronizar")
    parser.add_argument("--synonyms", action="store_true",
                        help="Solo sincronizar sinonimos de busqueda")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Tamano del batch para upsert (default: 500)")
    args = parser.parse_args()

    # Ensure DB tables exist
    from app.db.engine import engine
    from app.db.models import Base
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    from app.services.typesense_service import (
        company_to_document,
        drop_collection,
        ensure_collection,
        sync_synonyms,
        upsert_documents,
    )

    # --- Solo sinonimos ---
    if args.synonyms:
        await ensure_collection()
        count = await sync_synonyms()
        print(f"Sincronizados {count} grupos de sinonimos")
        return

    # --- Recrear coleccion ---
    if args.recreate:
        print("Eliminando coleccion...")
        await drop_collection()

    print("Asegurando coleccion...")
    created = await ensure_collection()
    if created:
        print("Coleccion creada. Sincronizando sinonimos...")
        await sync_synonyms()

    # --- Query empresas desde SQLite ---
    from app.db.engine import async_session

    async with async_session() as db:
        query = select(Company)

        if args.since:
            since_date = datetime.fromisoformat(args.since)
            query = query.where(Company.updated_at >= since_date)
            print(f"Sync incremental: empresas actualizadas desde {args.since}")
        else:
            print("Sync completo: todas las empresas")

        # Count
        count_query = select(func.count(Company.id))
        if args.since:
            count_query = count_query.where(
                Company.updated_at >= datetime.fromisoformat(args.since)
            )
        total = await db.scalar(count_query) or 0
        print(f"Empresas a sincronizar: {total:,}")

        if total == 0:
            print("Nada que sincronizar.")
            return

        # Stream en batches
        t0 = time.monotonic()
        offset = 0
        synced = 0
        errors = 0
        batch_size = args.batch_size

        while offset < total:
            batch_query = query.order_by(Company.id).offset(offset).limit(batch_size)
            result = await db.scalars(batch_query)
            companies = result.all()

            if not companies:
                break

            docs = [company_to_document(c) for c in companies]
            stats = await upsert_documents(docs, batch_size=200)
            synced += stats["success"]
            errors += stats["errors"]
            offset += len(companies)

            pct = min(100, round(offset / total * 100))
            print(f"  [{pct:3d}%] {offset:,}/{total:,} procesadas ({stats['success']} ok, {stats['errors']} err)")

    elapsed = time.monotonic() - t0
    print(f"\nSync completado en {elapsed:.1f}s")
    print(f"  Total: {synced:,} sincronizadas, {errors:,} errores")


if __name__ == "__main__":
    asyncio.run(main())
