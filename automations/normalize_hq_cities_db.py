#!/usr/bin/env python3
import argparse
import json
import os
import re
import sqlite3
import difflib
import unicodedata
from typing import Dict, Tuple

try:
    import psycopg
except Exception:
    psycopg = None
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


DB_PATH_DEFAULT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "db", "rounds.db")

CITY_ALIAS_TO_EN = {
    "milano": "Milan",
    "milan": "Milan",
    "torino": "Turin",
    "turin": "Turin",
    "roma": "Rome",
    "rome": "Rome",
    "napoli": "Naples",
    "naples": "Naples",
    "firenze": "Florence",
    "florence": "Florence",
    "venezia": "Venice",
    "venice": "Venice",
    "genova": "Genoa",
    "genoa": "Genoa",
    "padova": "Padua",
    "padua": "Padua",
    "bologna": "Bologna",
    "bergamo": "Bergamo",
    "parma": "Parma",
    "pisa": "Pisa",
    "modena": "Modena",
    "trento": "Trento",
    "trieste": "Trieste",
    "brescia": "Brescia",
    "verona": "Verona",
    "vicenza": "Vicenza",
    "poggibonsi": "Poggibonsi",
    "bovisio": "Bovisio",
    "laquila": "L'Aquila",
    "l aquila": "L'Aquila",
    "lacquila": "L'Aquila",
    "l acquila": "L'Aquila",
    "berlino": "Berlin",
}
def is_generic_hq(value: str) -> bool:
    v = str(value or "").strip().lower()
    return v in ("", "italy", "italia", "<city>", "city", "unknown", "n/a", "na", "nd")


def _normalize_city_key(value: str) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _is_non_city_token(token: str) -> bool:
    k = _normalize_city_key(token)
    if not k:
        return True
    if k in {
        "italy", "italia", "us", "usa", "uk", "eu", "europe",
        "veneto", "apulia", "sud sardegna",
    }:
        return True
    bad_fragments = (
        "provincia", "region", "county", "state", "country", "metropolitan",
    )
    return any(f in k for f in bad_fragments)


def normalize_city_name(value: str) -> str:
    s = str(value or "").strip()
    if not s or is_generic_hq(s):
        return ""
    s = re.sub(r"\([^)]*\)", " ", s)
    parts = [p.strip() for p in re.split(r"\s*/\s*|\s*-\s*|\s*\|\s*|,\s*", s) if p.strip()]
    if not parts:
        parts = [s]
    normalized_parts = []
    for p in parts:
        key = _normalize_city_key(p)
        if not key:
            continue
        if _is_non_city_token(p):
            continue
        if key in CITY_ALIAS_TO_EN:
            normalized_parts.append(CITY_ALIAS_TO_EN[key])
            continue
        close = difflib.get_close_matches(key, CITY_ALIAS_TO_EN.keys(), n=1, cutoff=0.75)
        if close:
            normalized_parts.append(CITY_ALIAS_TO_EN[close[0]])
            continue
        normalized_parts.append(re.sub(r"\s+", " ", p).strip(" .,-").title())
    if not normalized_parts:
        return ""
    seen = set()
    out = []
    for c in normalized_parts:
        k = c.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(c)
    return " / ".join(out)


def connect(db_path: str, database_url: str):
    if database_url:
        if psycopg is None:
            raise RuntimeError("psycopg is not installed but --database-url was provided")
        return psycopg.connect(database_url), True
    return sqlite3.connect(db_path), False


def ensure_overrides_table(conn, pg_mode: bool):
    cur = conn.cursor()
    try:
        if pg_mode:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS public.hq_overrides (
                  "Company" text PRIMARY KEY,
                  "HQ" text NOT NULL
                )
                """
            )
        else:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS hq_overrides (
                  "Company" TEXT PRIMARY KEY,
                  "HQ" TEXT NOT NULL
                )
                """
            )
        conn.commit()
    finally:
        cur.close()


def read_company_city_stats(conn) -> Dict[str, Dict[str, Tuple[int, int]]]:
    cur = conn.cursor()
    try:
        cur.execute('SELECT id, "Company", "HQ" FROM rounds WHERE COALESCE("Company", \'\') <> \'\'')
        stats: Dict[str, Dict[str, Tuple[int, int]]] = {}
        for rid, company, hq in cur.fetchall():
            ck = str(company or "").strip().lower()
            city = normalize_city_name(hq)
            if not ck or not city:
                continue
            stats.setdefault(ck, {})
            cnt, last_id = stats[ck].get(city, (0, -1))
            stats[ck][city] = (cnt + 1, max(last_id, int(rid or 0)))
        return stats
    finally:
        cur.close()


def choose_canonical_city(stats: Dict[str, Dict[str, Tuple[int, int]]], overrides: Dict[str, str]) -> Dict[str, str]:
    canonical: Dict[str, str] = {}
    for company_key, cities in stats.items():
        if company_key in overrides and normalize_city_name(overrides[company_key]):
            canonical[company_key] = normalize_city_name(overrides[company_key])
            continue
        chosen = sorted(cities.items(), key=lambda kv: (kv[1][0], kv[1][1]), reverse=True)[0][0]
        canonical[company_key] = chosen
    return canonical


def resolve_conflicts_with_ai(
    stats: Dict[str, Dict[str, Tuple[int, int]]],
    canonical: Dict[str, str],
    model: str = "gpt-4.1-mini",
) -> Dict[str, str]:
    if OpenAI is None:
        return {}
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return {}
    conflicts = []
    for company_key, city_stats in stats.items():
        if len(city_stats) <= 1:
            continue
        options = sorted(city_stats.keys())
        conflicts.append({"company": company_key, "options": options})
    if not conflicts:
        return {}
    client = OpenAI(api_key=api_key)
    system = (
        "Choose the correct headquarters city for each company using reliable public sources "
        "(official website, LinkedIn company page). "
        "Return JSON array only with items: company, city. "
        "City must be one of the provided options; if uncertain return empty city."
    )
    user = {"items": conflicts}
    try:
        payload = {
            "model": model,
            "input": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user, ensure_ascii=False)},
            ],
        }
        try:
            resp = client.responses.create(**payload, tools=[{"type": "web_search_preview"}])
        except Exception:
            resp = client.responses.create(**payload)
        text = resp.output_text if hasattr(resp, "output_text") else ""
        parsed = json.loads(text) if text else []
        updates: Dict[str, str] = {}
        if not isinstance(parsed, list):
            return updates
        for item in parsed:
            if not isinstance(item, dict):
                continue
            company = str(item.get("company", "")).strip().lower()
            city = normalize_city_name(item.get("city", ""))
            if not company or not city:
                continue
            if company in stats and city in stats[company]:
                updates[company] = city
        return updates
    except Exception:
        return {}


def apply_rounds_updates(conn, pg_mode: bool, canonical: Dict[str, str], dry_run: bool):
    p = "%s" if pg_mode else "?"
    cur = conn.cursor()
    try:
        cur.execute('SELECT id, "Company", "HQ" FROM rounds WHERE COALESCE("Company", \'\') <> \'\'')
        touched = 0
        updated = 0
        for rid, company, hq in cur.fetchall():
            ck = str(company or "").strip().lower()
            if not ck or ck not in canonical:
                continue
            touched += 1
            target = canonical[ck]
            current = normalize_city_name(hq)
            if current != target:
                updated += 1
                if not dry_run:
                    cur.execute(f'UPDATE rounds SET "HQ" = {p} WHERE id = {p}', (target, rid))
        if not dry_run:
            conn.commit()
        return touched, updated
    finally:
        cur.close()


def upsert_overrides(conn, pg_mode: bool, canonical: Dict[str, str], dry_run: bool):
    if dry_run:
        return
    p = "%s" if pg_mode else "?"
    cur = conn.cursor()
    try:
        for company_key, city in canonical.items():
            if pg_mode:
                cur.execute(
                    f'INSERT INTO public.hq_overrides ("Company","HQ") VALUES ({p},{p}) '
                    f'ON CONFLICT ("Company") DO UPDATE SET "HQ" = EXCLUDED."HQ"',
                    (company_key, city),
                )
            else:
                cur.execute(
                    f'INSERT INTO hq_overrides ("Company","HQ") VALUES ({p},{p}) '
                    f'ON CONFLICT("Company") DO UPDATE SET "HQ" = excluded."HQ"',
                    (company_key, city),
                )
        conn.commit()
    finally:
        cur.close()


def main():
    parser = argparse.ArgumentParser(description="Normalize HQ cities to English and unify per company.")
    parser.add_argument("--db", default=DB_PATH_DEFAULT, help=f"SQLite DB path (default: {DB_PATH_DEFAULT})")
    parser.add_argument("--database-url", default="", help="Postgres connection URL")
    parser.add_argument("--overrides-json", default="", help="Optional JSON company->city overrides (English city names)")
    parser.add_argument("--resolve-conflicts-ai", action="store_true", help="Use OpenAI+web search to resolve multi-city company conflicts")
    parser.add_argument("--ai-model", default="gpt-4.1-mini", help="OpenAI model for conflict resolution")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    overrides: Dict[str, str] = {}
    if args.overrides_json and os.path.exists(args.overrides_json):
        with open(args.overrides_json, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            for k, v in data.items():
                ck = str(k or "").strip().lower()
                cv = normalize_city_name(v)
                if ck and cv:
                    overrides[ck] = cv

    conn, pg_mode = connect(args.db, args.database_url.strip())
    try:
        ensure_overrides_table(conn, pg_mode)
        stats = read_company_city_stats(conn)
        canonical = choose_canonical_city(stats, overrides)
        if args.resolve_conflicts_ai:
            ai_updates = resolve_conflicts_with_ai(stats, canonical, args.ai_model)
            canonical.update(ai_updates)
        touched, updated = apply_rounds_updates(conn, pg_mode, canonical, args.dry_run)
        upsert_overrides(conn, pg_mode, canonical, args.dry_run)
    finally:
        conn.close()

    mode = "DRY RUN" if args.dry_run else "APPLIED"
    print(json.dumps({"status": mode, "companies": len(canonical), "rows_considered": touched, "rows_updated": updated}, ensure_ascii=False))


if __name__ == "__main__":
    main()
