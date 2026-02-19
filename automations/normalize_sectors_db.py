#!/usr/bin/env python3
import argparse
import json
import os
import sqlite3
from typing import Dict, Tuple

try:
    import psycopg
except Exception:
    psycopg = None


DB_PATH_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "rounds.db")

# Manual overrides for known edge-cases.
DEFAULT_OVERRIDES = {
    "bending spoons": "Enterprise Tech",
}


def read_overrides(path: str) -> Dict[str, str]:
    overrides = dict(DEFAULT_OVERRIDES)
    if not path:
        return overrides
    if not os.path.exists(path):
        return overrides
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        for k, v in data.items():
            ck = str(k).strip().lower()
            sv = str(v).strip()
            if ck and sv:
                overrides[ck] = sv
    return overrides


def connect(db_path: str, database_url: str):
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is not installed but --database-url was provided")
        return psycopg.connect(database_url), True
    return sqlite3.connect(db_path), False


def fetch_canonical_sector(conn, pg_mode: bool) -> Dict[str, str]:
    cur = conn.cursor()
    try:
        cur.execute(
            """
            WITH ranked AS (
              SELECT
                LOWER("Company") AS company_key,
                "Sector 1" AS sector,
                COUNT(*) AS cnt,
                MAX(id) AS last_id,
                ROW_NUMBER() OVER (
                  PARTITION BY LOWER("Company")
                  ORDER BY COUNT(*) DESC, MAX(id) DESC
                ) AS rn
              FROM rounds
              WHERE COALESCE("Company", '') <> ''
                AND COALESCE("Sector 1", '') <> ''
              GROUP BY LOWER("Company"), "Sector 1"
            )
            SELECT company_key, sector
            FROM ranked
            WHERE rn = 1
            """
        )
        out: Dict[str, str] = {}
        for company_key, sector in cur.fetchall():
            ck = str(company_key or "").strip()
            sv = str(sector or "").strip()
            if ck and sv:
                out[ck] = sv
        return out
    finally:
        cur.close()


def apply_normalization(conn, pg_mode: bool, canonical: Dict[str, str], dry_run: bool) -> Tuple[int, int]:
    p = "%s" if pg_mode else "?"
    cur = conn.cursor()
    try:
        cur.execute('SELECT id, "Company", "Sector 1" FROM rounds')
        rows = cur.fetchall()
        touched = 0
        updated = 0
        for rid, company, sector in rows:
            ck = str(company or "").strip().lower()
            if not ck:
                continue
            wanted = canonical.get(ck, "")
            if not wanted:
                continue
            touched += 1
            current = str(sector or "").strip()
            if current != wanted:
                updated += 1
                if not dry_run:
                    cur.execute(f'UPDATE rounds SET "Sector 1" = {p} WHERE id = {p}', (wanted, rid))
        if not dry_run:
            conn.commit()
        return touched, updated
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="Normalize Sector 1 by company across the full rounds DB.")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help=f"SQLite DB path (default: {DB_PATH_DEFAULT})")
    parser.add_argument("--database-url", default="", help="Postgres connection URL")
    parser.add_argument("--overrides-json", default="", help="Optional JSON file with company->sector overrides")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn, pg_mode = connect(args.db, args.database_url.strip())
    try:
        canonical = fetch_canonical_sector(conn, pg_mode)
        overrides = read_overrides(args.overrides_json)
        canonical.update(overrides)
        touched, updated = apply_normalization(conn, pg_mode, canonical, args.dry_run)
    finally:
        conn.close()

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    print(json.dumps({"status": mode, "rows_considered": touched, "rows_updated": updated}, ensure_ascii=False))


if __name__ == "__main__":
    main()
