"""Seed the database with reference data."""
import asyncio
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.db.seed_cnae import seed_all

if __name__ == "__main__":
    asyncio.run(seed_all())
