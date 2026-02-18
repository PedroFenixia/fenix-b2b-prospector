#!/usr/bin/env python3
"""Remove false-positive company entries created by BORME parser bug.

The parser's company header regex was incorrectly matching numbered
sub-entries inside 'Datos registrales' blocks, creating fake company
records with names like '(BARCELONA). Datos registrales...',
'(31.08.22)', etc.

This script deletes those entries (and their related acts/officers via CASCADE).

Usage:
  python scripts/cleanup_false_companies.py --pg "postgresql://fenix:PASS@localhost:5432/fenix_prospector"

Or for SQLite:
  python scripts/cleanup_false_companies.py --sqlite data/prospector.db
"""
import argparse
import sys


def cleanup_pg(conn_str: str, dry_run: bool = False):
    import psycopg2

    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    # Count false positives
    cur.execute("SELECT count(*) FROM companies WHERE nombre LIKE '(%'")
    count = cur.fetchone()[0]
    print(f"Found {count:,} false-positive companies (nombre starts with '(')")

    if count == 0:
        print("Nothing to clean up.")
        conn.close()
        return

    if dry_run:
        cur.execute("SELECT id, nombre, provincia FROM companies WHERE nombre LIKE '(%' LIMIT 20")
        print("\nSample entries to be deleted:")
        for row in cur.fetchall():
            print(f"  [{row[0]}] {row[1][:80]}  ({row[2]})")
        conn.close()
        return

    # Delete related records (CASCADE should handle this, but be explicit)
    cur.execute("DELETE FROM officers WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    officers_deleted = cur.rowcount
    cur.execute("DELETE FROM acts WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    acts_deleted = cur.rowcount
    cur.execute("DELETE FROM alerts WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    alerts_deleted = cur.rowcount
    cur.execute("DELETE FROM watchlist WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    watchlist_deleted = cur.rowcount
    cur.execute("DELETE FROM companies WHERE nombre LIKE '(%'")
    companies_deleted = cur.rowcount

    conn.commit()
    conn.close()

    print(f"Deleted: {companies_deleted:,} companies, {acts_deleted:,} acts, "
          f"{officers_deleted:,} officers, {alerts_deleted:,} alerts, "
          f"{watchlist_deleted:,} watchlist entries")


def cleanup_sqlite(db_path: str, dry_run: bool = False):
    import sqlite3

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT count(*) FROM companies WHERE nombre LIKE '(%'")
    count = cur.fetchone()[0]
    print(f"Found {count:,} false-positive companies (nombre starts with '(')")

    if count == 0:
        print("Nothing to clean up.")
        conn.close()
        return

    if dry_run:
        cur.execute("SELECT id, nombre, provincia FROM companies WHERE nombre LIKE '(%' LIMIT 20")
        print("\nSample entries to be deleted:")
        for row in cur.fetchall():
            print(f"  [{row[0]}] {row[1][:80]}  ({row[2]})")
        conn.close()
        return

    cur.execute("DELETE FROM officers WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    officers_deleted = cur.rowcount
    cur.execute("DELETE FROM acts WHERE company_id IN (SELECT id FROM companies WHERE nombre LIKE '(%')")
    acts_deleted = cur.rowcount
    cur.execute("DELETE FROM companies WHERE nombre LIKE '(%'")
    companies_deleted = cur.rowcount

    conn.commit()
    conn.close()

    print(f"Deleted: {companies_deleted:,} companies, {acts_deleted:,} acts, "
          f"{officers_deleted:,} officers")


def main():
    parser = argparse.ArgumentParser(description="Cleanup false-positive companies")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pg", help="PostgreSQL connection string")
    group.add_argument("--sqlite", help="Path to SQLite DB file")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be deleted without deleting")
    args = parser.parse_args()

    if args.pg:
        cleanup_pg(args.pg, dry_run=args.dry_run)
    else:
        cleanup_sqlite(args.sqlite, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
