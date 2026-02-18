#!/usr/bin/env python3
"""Migrate data from SQLite to PostgreSQL.

Usage:
  python scripts/migrate_sqlite_to_pg.py \
    --sqlite data/prospector.db \
    --pg "postgresql://fenix:PASSWORD@localhost:5432/fenix_prospector"
"""
import argparse
import sqlite3
import time

import psycopg2

# Table migration order (respects foreign keys)
TABLES_IN_ORDER = [
    "cnae_codes",
    "provinces",
    "users",
    "companies",
    "acts",
    "officers",
    "ingestion_log",
    "subsidies",
    "tenders",
    "judicial_notices",
    "watchlist",
    "act_type_watches",
    "alerts",
    "api_keys",
    "export_log",
    "erp_connections",
    "erp_sync_log",
]

# Boolean columns that need conversion (0/1 in SQLite -> True/False in PG)
BOOLEAN_COLUMNS = {
    "users": ["email_verified", "phone_verified", "is_active"],
    "act_type_watches": ["is_active"],
    "alerts": ["leida"],
    "api_keys": ["is_active"],
    "erp_connections": ["is_active"],
}

# Columns to skip (PG-only, not in SQLite)
SKIP_COLUMNS = {
    "companies": ["search_vector"],
}

BATCH_SIZE = 10_000


def migrate_table(sqlite_conn, pg_conn, table_name):
    """Migrate a single table."""
    src = sqlite_conn.cursor()

    # Check if table exists in SQLite
    src.execute(f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    if src.fetchone()[0] == 0:
        print(f"  {table_name}: SKIPPED (not in SQLite)")
        return 0

    src.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in src.description]

    # Filter out PG-only columns
    skip = set(SKIP_COLUMNS.get(table_name, []))

    bool_cols = BOOLEAN_COLUMNS.get(table_name, [])
    bool_indices = [columns.index(c) for c in bool_cols if c in columns]

    pg_cur = pg_conn.cursor()

    # Disable triggers for speed
    pg_cur.execute(f"ALTER TABLE {table_name} DISABLE TRIGGER ALL")

    total = 0
    while True:
        rows = src.fetchmany(BATCH_SIZE)
        if not rows:
            break

        # Filter columns and convert booleans
        filtered_cols = [c for c in columns if c not in skip]
        filtered_indices = [columns.index(c) for c in filtered_cols]

        processed_rows = []
        for row in rows:
            new_row = list(row[i] for i in filtered_indices)
            # Convert booleans
            for bc in bool_cols:
                if bc in filtered_cols:
                    idx = filtered_cols.index(bc)
                    new_row[idx] = bool(new_row[idx]) if new_row[idx] is not None else False
            processed_rows.append(tuple(new_row))

        cols_str = ", ".join(f'"{c}"' for c in filtered_cols)
        placeholders = ", ".join(["%s"] * len(filtered_cols))
        insert_sql = f'INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING'

        pg_cur.executemany(insert_sql, processed_rows)
        total += len(processed_rows)
        print(f"  {table_name}: {total:,} rows...", end="\r")

    # Re-enable triggers
    pg_cur.execute(f"ALTER TABLE {table_name} ENABLE TRIGGER ALL")
    pg_conn.commit()

    # Reset sequence to max id
    if "id" in columns:
        pg_cur.execute(f"""
            SELECT setval(pg_get_serial_sequence('{table_name}', 'id'),
                          COALESCE((SELECT MAX(id) FROM {table_name}), 0) + 1, false)
        """)
        pg_conn.commit()

    # Reset sequence for string PKs (cnae_codes, provinces don't have serial id)
    print(f"  {table_name}: DONE ({total:,} rows)")
    return total


def populate_search_vectors(pg_conn, fts_config="fenix_spanish"):
    """Populate the search_vector column for all companies."""
    print("Populating search_vector column...")
    cur = pg_conn.cursor()
    cur.execute(f"""
        UPDATE companies SET search_vector =
            setweight(to_tsvector('{fts_config}', COALESCE(nombre_normalizado, '')), 'A') ||
            setweight(to_tsvector('{fts_config}', COALESCE(objeto_social, '')), 'B')
        WHERE search_vector IS NULL
    """)
    pg_conn.commit()
    print(f"  Updated {cur.rowcount:,} companies with search vectors")


def main():
    parser = argparse.ArgumentParser(description="Migrate SQLite to PostgreSQL")
    parser.add_argument("--sqlite", required=True, help="Path to SQLite DB file")
    parser.add_argument("--pg", required=True, help="PostgreSQL connection string")
    parser.add_argument("--fts-config", default="fenix_spanish", help="PG FTS config name")
    args = parser.parse_args()

    print(f"Connecting to SQLite: {args.sqlite}")
    sqlite_conn = sqlite3.connect(args.sqlite)

    print(f"Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(args.pg)

    t0 = time.time()
    print("Starting migration...\n")

    grand_total = 0
    for table in TABLES_IN_ORDER:
        try:
            count = migrate_table(sqlite_conn, pg_conn, table)
            grand_total += count
        except Exception as e:
            print(f"  ERROR migrating {table}: {e}")
            pg_conn.rollback()

    print()
    populate_search_vectors(pg_conn, args.fts_config)

    # VACUUM ANALYZE
    print("Running VACUUM ANALYZE...")
    pg_conn.autocommit = True
    cur = pg_conn.cursor()
    cur.execute("VACUUM ANALYZE")

    elapsed = time.time() - t0
    print(f"\nMigration complete: {grand_total:,} total rows in {elapsed:.1f}s")

    sqlite_conn.close()
    pg_conn.close()


if __name__ == "__main__":
    main()
