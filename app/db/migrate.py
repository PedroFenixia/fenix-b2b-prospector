"""Auto-migration: detect and add missing columns on startup.

SQLAlchemy's create_all() only creates new tables—it won't ALTER existing
ones.  This module compares the ORM model definitions with the live DB
schema and issues ALTER TABLE … ADD COLUMN for any gaps.
"""

import logging
from sqlalchemy import inspect as sa_inspect, text
from app.db.models import Base

logger = logging.getLogger("fenix.migrate")


async def auto_migrate(engine) -> None:
    """Add columns defined in models but missing from the DB."""
    is_pg = "postgresql" in engine.url.drivername

    # 1. Snapshot current DB schema
    async with engine.connect() as conn:
        def _inspect(sync_conn):
            insp = sa_inspect(sync_conn)
            return {
                tbl: {c["name"] for c in insp.get_columns(tbl)}
                for tbl in insp.get_table_names()
            }
        db_schema = await conn.run_sync(_inspect)

    # 2. Compare with ORM models and build ALTER statements
    stmts: list[str] = []
    for table in Base.metadata.sorted_tables:
        if table.name not in db_schema:
            continue  # New table → create_all handles it
        existing_cols = db_schema[table.name]
        for col in table.columns:
            if col.name in existing_cols:
                continue
            col_type = col.type.compile(dialect=engine.dialect)
            parts = [f"ALTER TABLE {table.name} ADD COLUMN"]
            if is_pg:
                parts.append("IF NOT EXISTS")
            parts.append(f"{col.name} {col_type}")
            # Default value
            if col.default is not None and hasattr(col.default, "arg"):
                arg = col.default.arg
                if isinstance(arg, bool):
                    parts.append(f"DEFAULT {'true' if arg else 'false'}" if is_pg else f"DEFAULT {1 if arg else 0}")
                elif isinstance(arg, (int, float)):
                    parts.append(f"DEFAULT {arg}")
                elif isinstance(arg, str):
                    parts.append(f"DEFAULT '{arg}'")
            elif col.nullable:
                parts.append("DEFAULT NULL")
            else:
                parts.append("DEFAULT 0" if "INT" in col_type.upper() else "DEFAULT ''")
            stmts.append(" ".join(parts))

    # 3. Execute migrations
    if stmts:
        async with engine.begin() as conn:
            for sql in stmts:
                logger.info("Auto-migrate: %s", sql)
                await conn.execute(text(sql))
        logger.info("Auto-migrate: %d column(s) added.", len(stmts))
    else:
        logger.debug("Auto-migrate: schema up to date.")
