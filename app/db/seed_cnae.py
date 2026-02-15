"""Seed CNAE codes and provinces into the database."""
import asyncio
import json
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.engine import async_session, engine
from app.db.models import Base, CnaeCode, Province

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "data"


async def seed_all():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with async_session() as db:
        await seed_cnae(db)
        await seed_provinces(db)
        await db.commit()
    print("Database seeded successfully.")


async def seed_cnae(db: AsyncSession):
    existing = await db.scalar(select(CnaeCode.code).limit(1))
    if existing:
        print("CNAE codes already seeded, skipping.")
        return

    with open(DATA_DIR / "cnae_codes.json", encoding="utf-8") as f:
        codes = json.load(f)

    for c in codes:
        db.add(CnaeCode(
            code=c["code"],
            description_es=c["description_es"],
            section=c.get("section"),
            division=c.get("division"),
        ))
    print(f"Seeded {len(codes)} CNAE codes.")


async def seed_provinces(db: AsyncSession):
    existing = await db.scalar(select(Province.code).limit(1))
    if existing:
        print("Provinces already seeded, skipping.")
        return

    with open(DATA_DIR / "provinces.json", encoding="utf-8") as f:
        provinces = json.load(f)

    for p in provinces:
        db.add(Province(
            code=p["code"],
            nombre=p["nombre"],
            comunidad=p["comunidad"],
        ))
    print(f"Seeded {len(provinces)} provinces.")


if __name__ == "__main__":
    asyncio.run(seed_all())
