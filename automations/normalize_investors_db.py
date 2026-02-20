#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import unicodedata

try:
    import psycopg
except Exception:
    psycopg = None


DB_PATH_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "rounds.db")
INVESTOR_COLS = ["Lead", "Co-lead / follow 1", "follow 2", "follow 3", "follow 4", "Debt"]
INVESTOR_ALIASES = {
    "cdp": "CDP Venture Capital",
    "cdpvc": "CDP Venture Capital",
    "cdp vc": "CDP Venture Capital",
    "cdpventurecapital": "CDP Venture Capital",
    "cdp venture capital": "CDP Venture Capital",
    "cdp venture capital sgr": "CDP Venture Capital",
    "cdpventurecapitalsgr": "CDP Venture Capital",
}


def connect(db_path: str, database_url: str):
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is not installed but --database-url was provided")
        return psycopg.connect(database_url), True
    return sqlite3.connect(db_path), False


def normalize_key(value: str) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def normalize_investor_name(value: str) -> str:
    s = re.sub(r"\s+", " ", str(value or "").strip())
    if not s:
        return ""
    parts = [p.strip() for p in re.split(r"\s*/\s*|\s*;\s*|\s*,\s*", s) if p.strip()]
    if not parts:
        parts = [s]
    out = []
    seen = set()
    for p in parts:
        key = normalize_key(p)
        if not key:
            continue
        canon = INVESTOR_ALIASES.get(key, re.sub(r"\s+", " ", p).strip(" .,-"))
        if not canon:
            continue
        lk = canon.lower()
        if lk in seen:
            continue
        seen.add(lk)
        out.append(canon)
    return " / ".join(out)


def main():
    parser = argparse.ArgumentParser(description="Normalize investor names across Lead/Co-lead/follow columns.")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help=f"SQLite DB path (default: {DB_PATH_DEFAULT})")
    parser.add_argument("--database-url", default="", help="Postgres connection URL")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    conn, pg_mode = connect(args.db, args.database_url.strip())
    p = "%s" if pg_mode else "?"
    touched = 0
    updated = 0
    try:
        cur = conn.cursor()
        select_cols = ", ".join([f'"{c}"' for c in INVESTOR_COLS])
        cur.execute(f'SELECT id, {select_cols} FROM rounds')
        rows = cur.fetchall()
        for row in rows:
            rid = row[0]
            current = {INVESTOR_COLS[i]: row[i + 1] for i in range(len(INVESTOR_COLS))}
            next_values = {k: normalize_investor_name(v) for k, v in current.items()}
            touched += 1
            if all((str(current[k] or "") == str(next_values[k] or "")) for k in INVESTOR_COLS):
                continue
            updated += 1
            if not args.dry_run:
                sets = ", ".join([f'"{c}" = {p}' for c in INVESTOR_COLS])
                params = [next_values[c] for c in INVESTOR_COLS] + [rid]
                cur.execute(f'UPDATE rounds SET {sets} WHERE id = {p}', params)
        if not args.dry_run:
            conn.commit()
        cur.close()
    finally:
        conn.close()

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    print(json.dumps({"status": mode, "rows_considered": touched, "rows_updated": updated}, ensure_ascii=False))


if __name__ == "__main__":
    main()
