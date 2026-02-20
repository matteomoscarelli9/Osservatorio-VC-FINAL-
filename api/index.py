import json
import os
import re
import sqlite3
import difflib
import subprocess
import importlib.util
import inspect
import hashlib
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request
from openai import OpenAI
try:
    import psycopg
except Exception:
    psycopg = None


app = Flask(__name__)

DEFAULT_DB_PATH = str(Path(__file__).resolve().parents[1] / "db" / "rounds.db")
DB_PATH = os.environ.get("DB_PATH", DEFAULT_DB_PATH)
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()
USE_POSTGRES = bool(DATABASE_URL)
AUTOMATION_SCRIPT = str(Path(__file__).resolve().parents[1] / "automations" / "dealflowit_to_excel.py")
EXTRACTION_MODEL = os.environ.get("OPENAI_EXTRACTION_MODEL", "gpt-5.2")

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
ITALIAN_CITY_EN = {
    "Milan", "Turin", "Rome", "Naples", "Florence", "Venice", "Genoa", "Padua",
    "Bologna", "Bergamo", "Parma", "Pisa", "Modena", "Trento", "Trieste", "Brescia",
    "Verona", "Vicenza", "Poggibonsi", "Bovisio",
}
SECTOR_OVERRIDES = {
    "bending spoons": "Enterprise Tech",
}
INVESTOR_ALIASES = {
    "cdp": "CDP Venture Capital",
    "cdpvc": "CDP Venture Capital",
    "cdp vc": "CDP Venture Capital",
    "cdpventurecapital": "CDP Venture Capital",
    "cdp venture capital": "CDP Venture Capital",
    "cdp venture capital sgr": "CDP Venture Capital",
    "cdpventurecapitalsgr": "CDP Venture Capital",
}
INVESTOR_COLS = ["Lead", "Co-lead / follow 1", "follow 2", "follow 3", "follow 4", "Debt"]


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


@app.route("/api/chat", methods=["OPTIONS"])
def chat_options():
    return ("", 204)

@app.route("/api/health", methods=["GET"])
def health():
    try:
        ensure_db()
        return jsonify({"status": "ok", "db": "ready"})
    except Exception as e:
        return jsonify({"status": "degraded", "error": str(e)}), 500


def db_conn():
    if USE_POSTGRES:
        if psycopg is None:
            raise RuntimeError("psycopg is not installed but DATABASE_URL is set")
        return psycopg.connect(DATABASE_URL)
    return sqlite3.connect(DB_PATH)


def ph():
    return "%s" if USE_POSTGRES else "?"


def round_size_expr_sql() -> str:
    if USE_POSTGRES:
        return (
            'CAST(NULLIF('
            "REGEXP_REPLACE(REPLACE(REPLACE(COALESCE(\"Round size (€M)\", ''), ',', '.'), '€', ''), '[^0-9.\\-]', '', 'g')"
            ", '') AS DOUBLE PRECISION)"
        )
    return (
        'CAST(NULLIF('
        'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE("Round size (€M)", \'\'), \',\', \'.\'), \'€\', \'\'), \' \', \'\'), \'M\', \'\'), \'m\', \'\')'
        ", '') AS REAL)"
    )


def parse_filter_number(value: str):
    s = str(value or "").strip()
    if not s:
        return None
    s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _normalize_investor_key(value: str) -> str:
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
        key = _normalize_investor_key(p)
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


def parse_amount_value(value) -> float:
    s = str(value or "").strip()
    if not s:
        return 0.0
    s = s.replace("€", "").replace("M", "").replace("m", "").replace(" ", "")
    if "," in s and "." in s:
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s) if s else 0.0
    except Exception:
        return 0.0


def extract_year(value: str):
    m = re.search(r"\b(20\d{2})\b", str(value or ""))
    return m.group(1) if m else None


def is_generic_hq(value: str) -> bool:
    v = str(value or "").strip().lower()
    return v in ("", "italy", "italia", "<city>", "city", "unknown", "n/a", "na", "nd")


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


def _normalize_city_key(value: str) -> str:
    s = str(value or "").strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _city_from_single_token(token: str) -> str:
    key = _normalize_city_key(token)
    if not key:
        return ""
    if _is_non_city_token(token):
        return ""
    if key in CITY_ALIAS_TO_EN:
        return CITY_ALIAS_TO_EN[key]
    close = difflib.get_close_matches(key, CITY_ALIAS_TO_EN.keys(), n=1, cutoff=0.75)
    if close:
        return CITY_ALIAS_TO_EN[close[0]]
    cleaned = re.sub(r"\s+", " ", str(token).strip()).strip(" .,-")
    return cleaned.title() if cleaned else ""


def normalize_city_name(value: str) -> str:
    s = str(value or "").strip()
    if not s or is_generic_hq(s):
        return ""
    s = re.sub(r"\([^)]*\)", " ", s)
    parts = [p.strip() for p in re.split(r"\s*/\s*|\s*-\s*|\s*\|\s*|,\s*", s) if p.strip()]
    if not parts:
        parts = [s]
    normalized_parts = [c for c in (_city_from_single_token(p) for p in parts) if c]
    if not normalized_parts:
        return ""
    # Keep multi-city when present, canonicalized and deduplicated.
    seen = set()
    out = []
    for c in normalized_parts:
        key = c.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return " / ".join(out)


def build_company_hq_map(cur) -> dict:
    overrides = {}
    for table in ('public.hq_overrides', 'hq_overrides'):
        try:
            cur.execute(f'SELECT "Company", "HQ" FROM {table}')
            for company, hq in cur.fetchall():
                ck = str(company or "").strip().lower()
                city = normalize_city_name(hq)
                if ck and city:
                    overrides[ck] = city
            break
        except Exception:
            continue

    cur.execute('SELECT id, "Company", "HQ" FROM rounds WHERE COALESCE("Company", \'\') <> \'\'')
    stats = {}
    for rid, company, hq in cur.fetchall():
        ck = str(company or "").strip().lower()
        city = normalize_city_name(hq)
        if not ck or not city:
            continue
        stats.setdefault(ck, {})
        cnt, last_id = stats[ck].get(city, (0, -1))
        stats[ck][city] = (cnt + 1, max(last_id, int(rid or 0)))
    out = {}
    for ck, city_stats in stats.items():
        out[ck] = sorted(city_stats.items(), key=lambda kv: (kv[1][0], kv[1][1]), reverse=True)[0][0]
    out.update(overrides)
    return out


def build_company_sector_map(cur) -> dict:
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
    out = {}
    for company_key, sector in cur.fetchall():
        ck = str(company_key or "").strip().lower()
        sv = str(sector or "").strip()
        if ck and sv:
            out[ck] = sv
    out.update(SECTOR_OVERRIDES)
    return out


def apply_canonical_hq(rows: list, company_hq_map: dict) -> list:
    for r in rows:
        company = str(r.get("Company", "")).strip().lower()
        current = normalize_city_name(r.get("HQ", ""))
        if company and company in company_hq_map:
            r["HQ"] = company_hq_map[company]
        else:
            r["HQ"] = current
    return rows


def apply_canonical_sector(rows: list, company_sector_map: dict) -> list:
    for r in rows:
        company = str(r.get("Company", "")).strip().lower()
        if company and company in company_sector_map:
            r["Sector 1"] = company_sector_map[company]
    return rows


def apply_canonical_investors(rows: list) -> list:
    for r in rows:
        for col in INVESTOR_COLS:
            if col in r:
                r[col] = normalize_investor_name(r.get(col, ""))
    return rows


def infer_intent_from_question(question: str) -> dict:
    q = (question or "").strip()
    ql = q.lower()
    intent = {"metric": "count", "subject": "rounds", "group_by": None, "top_n": None, "filters": {}}

    if any(k in ql for k in ["how many rounds", "quanti round", "numero di round", "number of rounds"]):
        intent["subject"] = "rounds"
        intent["metric"] = "count"
    elif any(k in ql for k in ["how many companies", "quante societ", "numero di societ", "number of companies"]):
        intent["subject"] = "companies"
        intent["metric"] = "count"
    elif any(k in ql for k in ["totale raccolto", "total raised", "quanto raccolto", "somma raccolta", "raccolto"]):
        intent["subject"] = "amount"
        intent["metric"] = "sum"
    elif any(k in ql for k in ["media", "average", "avg"]):
        intent["subject"] = "amount"
        intent["metric"] = "avg"
    elif any(k in ql for k in ["massimo", "max", "largest", "biggest"]):
        intent["subject"] = "amount"
        intent["metric"] = "max"
    elif any(k in ql for k in ["minimo", "min", "smallest"]):
        intent["subject"] = "amount"
        intent["metric"] = "min"

    # Questions like "chi ha raccolto di più nel 2026?"
    if (
        any(k in ql for k in ["chi ", "who "])
        and any(k in ql for k in ["raccolto", "raised"])
        and any(k in ql for k in ["di più", "di piu", "most", "più", "piu"])
    ):
        intent["subject"] = "amount"
        intent["metric"] = "sum"
        intent["group_by"] = "company"
        intent["top_n"] = 1

    if (
        any(k in ql for k in ["top", "classifica", "ranking"])
        and any(k in ql for k in ["azienda", "aziende", "societ", "company", "companies"])
        and any(k in ql for k in ["raccolto", "raised", "funding"])
    ):
        intent["subject"] = "amount"
        intent["metric"] = "sum"
        intent["group_by"] = "company"
        if intent.get("top_n") is None:
            intent["top_n"] = 5

    if any(k in ql for k in ["per settore", "per settori", "by sector", "sectors"]) or ("settor" in ql):
        intent["group_by"] = "sector"
    elif any(k in ql for k in ["per citta", "per città", "by city", "cities"]):
        intent["group_by"] = "city"
    elif any(k in ql for k in ["per azienda", "by company"]):
        intent["group_by"] = "company"
    elif any(k in ql for k in ["per lead", "by lead"]):
        intent["group_by"] = "lead"
    elif any(k in ql for k in ["per anno", "by year"]):
        intent["group_by"] = "year"
    elif any(k in ql for k in ["per quarter", "per trimestre", "by quarter"]):
        intent["group_by"] = "quarter"

    m_top = re.search(r"\btop\s+(\d{1,3})\b", ql)
    if m_top:
        intent["top_n"] = int(m_top.group(1))

    m_q = re.search(r"\bq([1-4])\s*(20\d{2})\b", ql, flags=re.IGNORECASE)
    if m_q:
        intent["filters"]["quarter_eq"] = f"Q{m_q.group(1)} {m_q.group(2)}"
        intent["filters"]["year_eq"] = m_q.group(2)
    else:
        m_year = re.search(r"\b(20\d{2})\b", ql)
        if m_year:
            intent["filters"]["year_eq"] = m_year.group(1)

    m_between = re.search(r"\b(?:tra|between)\s*(20\d{2})\s*(?:e|and)\s*(20\d{2})\b", ql)
    if m_between:
        intent["filters"]["year_from"] = int(m_between.group(1))
        intent["filters"]["year_to"] = int(m_between.group(2))
        intent["filters"].pop("year_eq", None)

    m_from_today = re.search(r"\b(?:dal|da|from|since)\s*(20\d{2}).*(?:ad oggi|oggi|to date|today)\b", ql)
    if m_from_today:
        intent["filters"]["year_from"] = int(m_from_today.group(1))
        intent["filters"].pop("year_eq", None)

    m_from = re.search(r"\b(?:dal|da|from|since)\s*(20\d{2})\b", ql)
    if m_from and "year_from" not in intent["filters"]:
        intent["filters"]["year_from"] = int(m_from.group(1))
        intent["filters"].pop("year_eq", None)

    m_min = re.search(
        r"\b(?:minimo|almeno|at least|over|oltre|above|greater than)\s*€?\s*([0-9]+(?:[.,][0-9]+)?)\s*([mk])?\b",
        ql,
        flags=re.IGNORECASE,
    )
    if m_min:
        val = float(m_min.group(1).replace(",", "."))
        unit = (m_min.group(2) or "").lower()
        if unit == "k":
            val = val / 1000.0
        intent["filters"]["min_amount"] = val

    return intent


def normalize_intent(intent: dict, question: str) -> dict:
    allowed_metric = {"count", "sum", "avg", "max", "min"}
    allowed_subject = {"rounds", "amount", "companies"}
    allowed_group_by = {"company", "city", "sector", "year", "lead", "quarter", None}
    out = intent if isinstance(intent, dict) else {}
    metric = str(out.get("metric", "")).lower().strip() or "count"
    subject = str(out.get("subject", "")).lower().strip() or "rounds"
    group_by = out.get("group_by")
    group_by = str(group_by).lower().strip() if group_by else None
    top_n = out.get("top_n")
    filters = out.get("filters") if isinstance(out.get("filters"), dict) else {}

    if metric not in allowed_metric:
        metric = "count"
    if subject not in allowed_subject:
        subject = "rounds"
    if group_by not in allowed_group_by:
        group_by = None
    try:
        top_n = int(top_n) if top_n is not None else None
    except Exception:
        top_n = None
    if top_n is not None:
        top_n = max(1, min(top_n, 200))

    # Heuristic fallback/merge from raw question.
    heur = infer_intent_from_question(question)
    if not out:
        return heur
    if metric == "count" and heur.get("metric") in {"sum", "avg", "max", "min"}:
        metric = heur["metric"]
        subject = heur.get("subject", subject)
    if subject == "rounds" and heur.get("subject") == "companies":
        subject = "companies"
    if group_by is None and heur.get("group_by"):
        group_by = heur["group_by"]
    if top_n is None and heur.get("top_n"):
        top_n = heur["top_n"]
    for k, v in heur.get("filters", {}).items():
        filters.setdefault(k, v)

    return {"metric": metric, "subject": subject, "group_by": group_by, "top_n": top_n, "filters": filters}


def order_by_nulls_last(column: str, descending: bool = True) -> str:
    # PostgreSQL supports NULLS LAST directly.
    if USE_POSTGRES:
        return f"ORDER BY {column} {'DESC' if descending else 'ASC'} NULLS LAST"
    # SQLite-compatible fallback.
    if descending:
        return f"ORDER BY ({column} IS NULL), {column} DESC"
    return f"ORDER BY ({column} IS NULL), {column} ASC"


def get_rounds_columns(cur):
    if USE_POSTGRES:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'rounds'
            ORDER BY ordinal_position
            """
        )
        return [row[0] for row in cur.fetchall()]
    cur.execute("PRAGMA table_info(rounds)")
    return [row[1] for row in cur.fetchall()]


def ensure_db():
    conn = db_conn()
    cur = conn.cursor()
    if USE_POSTGRES:
        cur.execute("SELECT to_regclass('public.rounds')")
        exists = cur.fetchone()[0] is not None
    else:
        if not os.path.exists(DB_PATH):
            raise RuntimeError(f"DB not found at {DB_PATH}")
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rounds'")
        exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    if not exists:
        raise RuntimeError("Table 'rounds' not found in DB")


def _load_automation_module():
    spec = importlib.util.spec_from_file_location("dealflowit_to_excel", AUTOMATION_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load automation module: {AUTOMATION_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_automation(payload: dict):
    use_current = bool(payload.get("use_current", False))
    subject = str(payload.get("subject", "TWIS")).strip() or "TWIS"
    sender = str(payload.get("sender", "")).strip()
    rss_url = str(payload.get("rss_url", "")).strip()
    recent_days = int(payload.get("recent_days", 30))
    debug = bool(payload.get("debug", False))
    hq_model = str(payload.get("hq_model", os.environ.get("OPENAI_HQ_MODEL", "gpt-4.1-mini"))).strip()

    if not os.path.exists(AUTOMATION_SCRIPT):
        return jsonify({"status": "Error", "error": f"Automation script not found: {AUTOMATION_SCRIPT}"}), 500

    cmd = [
        "python3",
        AUTOMATION_SCRIPT,
        "--model",
        EXTRACTION_MODEL,
        "--hq-model",
        hq_model,
        "--subject",
        subject,
        "--recent-days",
        str(recent_days),
    ]
    if USE_POSTGRES:
        cmd.extend(["--database-url", DATABASE_URL])
    else:
        cmd.extend(["--db", DB_PATH])
    if sender:
        cmd.extend(["--sender", sender])
    if rss_url:
        cmd.extend(["--rss-url", rss_url])
    if use_current:
        cmd.append("--use-current")
    if debug:
        cmd.append("--debug")

    proc = subprocess.run(cmd, capture_output=True, text=True, env=os.environ.copy())
    run_stdout = (proc.stdout or "").strip()
    run_stderr = (proc.stderr or "").strip()
    if run_stdout:
        print(f"[api/run stdout]\n{run_stdout}", flush=True)
    if run_stderr:
        print(f"[api/run stderr]\n{run_stderr}", flush=True)
    if proc.returncode != 0:
        err = run_stderr or run_stdout
        if "osascript" in err.lower() or "microsoft outlook" in err.lower():
            err = (
                "Outlook automation unavailable in this runtime. "
                "Run /api/run from your local Mac environment where Outlook is installed and accessible."
            )
            return jsonify({"status": "Error", "error": err}), 501
        return jsonify({"status": "Error", "error": err or "Run failed"}), 500

    rows = 0
    companies = []
    for line in (proc.stdout or "").splitlines():
        if line.startswith("RESULT_JSON:"):
            try:
                data = json.loads(line.replace("RESULT_JSON:", "", 1))
                rows = int(data.get("rows", 0) or 0)
                companies = data.get("companies", []) or []
            except Exception:
                pass

    return jsonify(
        {
            "status": "Success",
            "rows": rows,
            "companies": companies,
            "model": EXTRACTION_MODEL,
            "time": datetime.now().strftime("%d %b %Y · %H:%M"),
        }
    )


@app.route("/api/run", methods=["POST"])
def run_job():
    payload = request.get_json(silent=True) or {}
    return _run_automation(payload)


@app.route("/api/cron/monday-run", methods=["GET"])
def monday_cron_run():
    cron_secret = os.environ.get("CRON_SECRET", "").strip()
    if cron_secret:
        auth = request.headers.get("authorization", "")
        if auth != f"Bearer {cron_secret}":
            return jsonify({"status": "Error", "error": "Unauthorized"}), 401

    # Enforce exact local schedule: only run at 09:10 Europe/Rome.
    now_rome = datetime.now(ZoneInfo("Europe/Rome"))
    if not (now_rome.weekday() == 0 and now_rome.hour == 9 and now_rome.minute == 10):
        return jsonify(
            {
                "status": "Skipped",
                "reason": "outside_target_local_time",
                "local_time_rome": now_rome.strftime("%Y-%m-%d %H:%M"),
            }
        )

    payload = {
        "subject": "TWIS",
        "recent_days": 30,
        "rss_url": "https://dealflowit.niccolosanarico.com/feed",
        "use_current": False,
        "debug": False,
    }
    return _run_automation(payload)


@app.route("/api/debug/rss-bullets", methods=["GET"])
def debug_rss_bullets():
    rss_url = request.args.get("rss_url", "https://dealflowit.niccolosanarico.com/feed").strip()
    subject = request.args.get("subject", "TWIS").strip() or "TWIS"
    recent_days = int(request.args.get("recent_days", "30"))
    try:
        mod = _load_automation_module()
        msg_subject, time_received, body = mod.fetch_latest_rss_message(rss_url, subject, recent_days)
        bullets = mod.extract_the_money_section(body)
        extractor_src = inspect.getsource(mod.extract_the_money_section)
        extractor_signature = hashlib.sha1(extractor_src.encode("utf-8")).hexdigest()[:12]
        return jsonify(
            {
                "status": "Success",
                "rss_url": rss_url,
                "subject_selected": msg_subject,
                "time_received": time_received,
                "extractor_signature": extractor_signature,
                "bullet_count": len(bullets),
                "bullets": bullets,
            }
        )
    except Exception as e:
        return jsonify({"status": "Error", "error": str(e)}), 500


@app.route("/api/sync", methods=["POST"])
def sync_endpoint():
    return jsonify(
        {
            "status": "Error",
            "error": "Unsupported on Vercel: /api/sync requires local Excel access."
        }
    ), 501


@app.route("/api/schema", methods=["GET"])
def schema():
    ensure_db()
    conn = db_conn()
    cur = conn.cursor()
    cols = get_rounds_columns(cur)
    cur.close()
    conn.close()
    return jsonify({"columns": cols})


@app.route("/api/rounds", methods=["GET"])
def rounds():
    ensure_db()
    limit = int(request.args.get("limit", "50"))
    offset = int(request.args.get("offset", "0"))
    search = request.args.get("search", "").strip()

    conn = db_conn()
    cur = conn.cursor()
    cols = get_rounds_columns(cur)

    company_hq_map = build_company_hq_map(cur)
    company_sector_map = build_company_sector_map(cur)

    if search:
        placeholder = ph()
        like_clause = " OR ".join(
            [f'LOWER(COALESCE(CAST("{c}" AS TEXT), \'\')) LIKE LOWER({placeholder})' for c in cols]
        )
        params = [f"%{search}%"] * len(cols)
        query = f"SELECT * FROM rounds WHERE {like_clause} ORDER BY id DESC LIMIT {placeholder} OFFSET {placeholder}"
        cur.execute(query, params + [limit, offset])
    else:
        placeholder = ph()
        cur.execute(
            f"SELECT * FROM rounds ORDER BY id DESC LIMIT {placeholder} OFFSET {placeholder}",
            (limit, offset),
        )

    rows = cur.fetchall()
    col_names = [description[0] for description in cur.description]
    conn.close()

    data = [dict(zip(col_names, row)) for row in rows]
    data = apply_canonical_hq(data, company_hq_map)
    data = apply_canonical_sector(data, company_sector_map)
    data = apply_canonical_investors(data)
    return jsonify({"rows": data, "columns": col_names})


@app.route("/api/stats", methods=["GET"])
def stats():
    ensure_db()
    conn = db_conn()
    cur = conn.cursor()
    company_hq_map = build_company_hq_map(cur)
    company_sector_map = build_company_sector_map(cur)
    cur.execute('SELECT "Company","HQ","Sector 1","Date","Round size (€M)" FROM rounds')
    rows = cur.fetchall()
    cur.close()
    conn.close()

    year_totals = {}
    year_counts = {}
    sector_totals = {}
    city_totals = {}

    for company, hq, sector, date_value, amount_raw in rows:
        amount = parse_amount_value(amount_raw)
        year = extract_year(date_value)
        if year:
            year_totals[year] = year_totals.get(year, 0.0) + amount
            year_counts[year] = year_counts.get(year, 0) + 1

        company_key = str(company or "").strip().lower()
        if company_key and company_key in company_sector_map:
            sector_key = company_sector_map[company_key]
        else:
            sector_key = str(sector or "").strip() or "Altro"
        sector_totals[sector_key] = sector_totals.get(sector_key, 0.0) + amount

        city = ""
        if company_key and company_key in company_hq_map:
            city = company_hq_map[company_key]
        else:
            city = normalize_city_name(hq)
        city_key = city or "ND"
        city_totals[city_key] = city_totals.get(city_key, 0.0) + amount

    years_sorted = sorted(year_totals.keys())
    totals_by_year = [{"year": y, "total": round(year_totals.get(y, 0.0), 6)} for y in years_sorted]
    rounds_by_year = [{"year": y, "count": int(year_counts.get(y, 0))} for y in years_sorted]
    top_sectors = [
        {"sector": k, "total": round(v, 6)}
        for k, v in sorted(sector_totals.items(), key=lambda kv: kv[1], reverse=True)[:6]
    ]
    top_cities = [
        {"city": k, "total": round(v, 6)}
        for k, v in sorted(city_totals.items(), key=lambda kv: kv[1], reverse=True)[:6]
    ]

    return jsonify(
        {
            "rows": len(rows),
            "totals_by_year": totals_by_year,
            "rounds_by_year": rounds_by_year,
            "top_sectors": top_sectors,
            "top_cities": top_cities,
            "checks": {
                "enterprise_tech_total": round(sector_totals.get("Enterprise Tech", 0.0), 6),
            },
        }
    )


@app.route("/api/rounds/query", methods=["POST"])
def rounds_query():
    ensure_db()
    payload = request.get_json(force=True)
    filters = payload.get("filters", {})
    limit = int(payload.get("limit", 50))
    offset = int(payload.get("offset", 0))

    conn = db_conn()
    cur = conn.cursor()
    cols = get_rounds_columns(cur)
    placeholder = ph()

    where_clauses = []
    params = []
    amount_expr = round_size_expr_sql()
    for col, value in filters.items():
        if col not in cols:
            continue
        if value is None or str(value).strip() == "":
            continue
        if col == "Round size (€M)":
            token = str(value)
            if token.startswith("lt:"):
                v = parse_filter_number(token.split(":", 1)[1])
                if v is not None:
                    where_clauses.append(f"{amount_expr} < {placeholder}")
                    params.append(v)
            elif token.startswith("gt:"):
                v = parse_filter_number(token.split(":", 1)[1])
                if v is not None:
                    where_clauses.append(f"{amount_expr} > {placeholder}")
                    params.append(v)
            elif token.startswith("between:"):
                parts = token.split(":")
                if len(parts) == 3:
                    lo = parse_filter_number(parts[1])
                    hi = parse_filter_number(parts[2])
                    if lo is not None and hi is not None:
                        where_clauses.append(f"{amount_expr} BETWEEN {placeholder} AND {placeholder}")
                        params.extend([lo, hi])
            else:
                v = parse_filter_number(token)
                if v is not None:
                    where_clauses.append(f"{amount_expr} = {placeholder}")
                    params.append(v)
        elif col == "HQ":
            city_norm = normalize_city_name(value)
            if city_norm:
                variants = {city_norm}
                # include known aliases mapping to same canonical city
                for k, v in CITY_ALIAS_TO_EN.items():
                    if v == city_norm:
                        variants.add(k.title())
                ors = [f'LOWER("HQ") = LOWER({placeholder})' for _ in variants]
                where_clauses.append("(" + " OR ".join(ors) + ")")
                params.extend(list(variants))
        else:
            where_clauses.append(f'LOWER("{col}") LIKE LOWER({placeholder})')
            params.append(f"%{value}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM rounds {where_sql} ORDER BY id DESC LIMIT {placeholder} OFFSET {placeholder}"
    cur.execute(query, params + [limit, offset])
    rows = cur.fetchall()
    col_names = [description[0] for description in cur.description]
    company_hq_map = build_company_hq_map(cur)
    company_sector_map = build_company_sector_map(cur)
    conn.close()
    data = [dict(zip(col_names, row)) for row in rows]
    data = apply_canonical_hq(data, company_hq_map)
    data = apply_canonical_sector(data, company_sector_map)
    data = apply_canonical_investors(data)
    return jsonify({"rows": data, "columns": col_names})


@app.route("/api/rounds/distinct", methods=["GET"])
def rounds_distinct():
    ensure_db()
    col = request.args.get("col", "").strip()
    if not col:
        return jsonify({"status": "Error", "error": "Missing col"}), 400

    if col == "Round size (€M)":
        buckets = ["lt:1", "between:1:3", "between:3:5", "between:5:10", "between:10:25", "between:25:100", "gt:100"]
        return jsonify({"column": col, "values": buckets})

    conn = db_conn()
    cur = conn.cursor()
    cols = get_rounds_columns(cur)
    if col not in cols:
        cur.close()
        conn.close()
        return jsonify({"status": "Error", "error": "Invalid col"}), 400

    try:
        if col == "HQ":
            company_hq_map = build_company_hq_map(cur)
            values = sorted(set(company_hq_map.values()))
            cur.close()
            conn.close()
            return jsonify({"column": col, "values": values})
        cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
        values = [row[0] for row in cur.fetchall() if row[0] not in (None, "")]
        if col in INVESTOR_COLS:
            values = sorted({normalize_investor_name(v) for v in values if normalize_investor_name(v)})
        cur.close()
        conn.close()
        return jsonify({"column": col, "values": values})
    except Exception as e:
        cur.close()
        conn.close()
        # Keep UI usable even if a single distinct query fails.
        return jsonify({"column": col, "values": [], "warning": f"distinct_failed: {e}"})


@app.route("/api/chat", methods=["POST"])
def chat():
    # In Vercel production we expect Supabase/Postgres as source of truth.
    if os.environ.get("VERCEL") and not USE_POSTGRES:
        return jsonify({"status": "Error", "error": "DATABASE_URL missing: chat is not connected to Supabase."}), 500

    ensure_db()
    payload = request.get_json(force=True)
    question = payload.get("question", "").strip()
    if not question:
        return jsonify({"status": "Error", "error": "Missing question"}), 400

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jsonify({"status": "Error", "error": "OPENAI_API_KEY missing"}), 400

    conn = db_conn()
    cur = conn.cursor()
    placeholder = ph()
    amount_expr = round_size_expr_sql()

    def distinct(col):
        cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
        return [r[0] for r in cur.fetchall() if r[0]]

    companies = distinct("Company")
    sectors = distinct("Sector 1")
    cities = distinct("HQ")
    leads = distinct("Lead")
    leads = sorted({normalize_investor_name(v) for v in leads if normalize_investor_name(v)})

    def normalize_text(val: str) -> str:
        return re.sub(r"[^a-z0-9\\s]", " ", val.lower()).strip()

    def map_entity(value, options):
        if not value:
            return None
        lower_map = {str(o).lower(): o for o in options}
        if value.lower() in lower_map:
            return lower_map[value.lower()]
        candidates = {normalize_text(str(o)): o for o in options}
        close = difflib.get_close_matches(normalize_text(value), list(candidates.keys()), n=1, cutoff=0.7)
        if close:
            return candidates[close[0]]
        return value

    client = OpenAI(api_key=api_key, timeout=20.0)
    intent = None
    try:
        system = (
            "Extract intent to query a database of Italian funding rounds. "
            "Return JSON only with keys: metric, subject, group_by, top_n, filters. "
            "metric: count|sum|avg|max|min. subject: rounds|amount|companies. "
            "group_by: company|city|sector|year|lead|quarter|null. "
            "filters may include: year_eq, year_from, year_to, quarter_eq, company, city, sector, lead, min_amount."
        )
        user = {"question": question}
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user, ensure_ascii=False)}],
        )
        raw = resp.output_text if hasattr(resp, "output_text") else ""
        intent = json.loads(raw)
    except Exception:
        intent = {}

    intent = normalize_intent(intent, question)
    metric = intent.get("metric")
    subject = intent.get("subject")
    group_by = intent.get("group_by") or None
    top_n = intent.get("top_n")
    filters = intent.get("filters") or {}

    filters["company"] = map_entity(filters.get("company"), companies)
    filters["sector"] = map_entity(filters.get("sector"), sectors)
    filters["city"] = map_entity(filters.get("city"), cities)
    filters["lead"] = map_entity(filters.get("lead"), leads)
    if not filters.get("company") and companies:
        q_norm = normalize_text(question)
        best = None
        for c in companies:
            c_text = str(c or "").strip()
            if not c_text:
                continue
            c_norm = normalize_text(c_text)
            if len(c_norm) < 3:
                continue
            if c_norm in q_norm:
                if best is None or len(c_norm) > len(best):
                    best = c_norm
                    filters["company"] = c_text
    if filters.get("min_amount") is not None:
        filters["min_amount"] = parse_filter_number(filters.get("min_amount"))

    clauses = []
    params = []
    year_expr = 'right("Date", 4)' if USE_POSTGRES else 'substr("Date", -4)'
    if filters.get("company"):
        clauses.append(f'LOWER("Company") LIKE LOWER({placeholder})')
        params.append(f'%{filters["company"]}%')
    if filters.get("sector"):
        clauses.append(f'LOWER("Sector 1") = LOWER({placeholder})')
        params.append(filters["sector"])
    if filters.get("city"):
        clauses.append(f'LOWER("HQ") = LOWER({placeholder})')
        params.append(filters["city"])
    if filters.get("lead"):
        clauses.append(f'LOWER("Lead") = LOWER({placeholder})')
        params.append(filters["lead"])
    if filters.get("year_from") and filters.get("year_to"):
        clauses.append(f"CAST({year_expr} AS INT) BETWEEN {placeholder} AND {placeholder}")
        params.extend([int(filters["year_from"]), int(filters["year_to"])])
    elif filters.get("year_from"):
        clauses.append(f"CAST({year_expr} AS INT) >= {placeholder}")
        params.append(int(filters["year_from"]))
    elif filters.get("year_eq"):
        clauses.append(f'"Date" LIKE {placeholder}')
        params.append(f'%{filters["year_eq"]}%')
    if filters.get("quarter_eq"):
        clauses.append(f'LOWER("Q") LIKE LOWER({placeholder})')
        params.append(f'%{filters["quarter_eq"]}%')
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    group_map = {
        "company": '"Company"',
        "city": '"HQ"',
        "sector": '"Sector 1"',
        "year": year_expr,
        "lead": '"Lead"',
        "quarter": '"Q"',
    }

    if subject == "companies" and metric == "count":
        if filters.get("min_amount") is not None:
            sql = (
                f'SELECT COUNT(*) AS company_count FROM ('
                f'SELECT "Company", SUM({amount_expr}) AS total_raised '
                f'FROM rounds {where_sql} GROUP BY "Company" '
                f'HAVING total_raised > {placeholder})'
            )
            params.append(filters["min_amount"])
        else:
            sql = f'SELECT COUNT(DISTINCT "Company") AS company_count FROM rounds {where_sql}'
    elif subject == "rounds" and metric == "count":
        if group_by in group_map:
            group_col = group_map[group_by]
            sql = (
                f'SELECT {group_col} AS group_key, COUNT(*) AS round_count '
                f'FROM rounds {where_sql} GROUP BY {group_col} '
                f'{order_by_nulls_last("round_count", descending=True)} LIMIT {top_n or 20}'
            )
        else:
            sql = f'SELECT COUNT(*) AS round_count FROM rounds {where_sql}'
    elif subject == "amount":
        agg_map = {"sum": "SUM", "avg": "AVG", "max": "MAX", "min": "MIN"}
        agg = agg_map.get(metric, "SUM")
        if group_by in group_map:
            group_col = group_map[group_by]
            order_desc = metric != "min"
            sql = (
                f'SELECT {group_col} AS group_key, {agg}({amount_expr}) AS total_raised '
                f'FROM rounds {where_sql} GROUP BY {group_col} '
                f'HAVING {agg}({amount_expr}) IS NOT NULL '
                f'{order_by_nulls_last("total_raised", descending=order_desc)} LIMIT {top_n or 20}'
            )
        else:
            sql = f'SELECT {agg}({amount_expr}) AS total_raised FROM rounds {where_sql} WHERE {amount_expr} IS NOT NULL' if not where_sql else f'SELECT {agg}({amount_expr}) AS total_raised FROM rounds {where_sql} AND {amount_expr} IS NOT NULL'
    else:
        sql = f'SELECT * FROM rounds {where_sql} ORDER BY id DESC LIMIT 200'

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        col_names = [description[0] for description in cur.description]
    except Exception as e:
        cur.close()
        conn.close()
        return jsonify({"status": "Error", "error": f"SQL error: {e}"}), 500
    cur.close()
    conn.close()

    if "round_count" in col_names and "group_key" in col_names:
        items = [f"{r[0]} ({r[1]})" for r in rows[:5] if r and r[0] not in (None, "")]
        return jsonify({"status": "Success", "answer": ("Top: " + ", ".join(items)) if items else "Nessun risultato."})
    if "round_count" in col_names:
        return jsonify({"status": "Success", "answer": f"Totale round: {rows[0][0] if rows else 0}"})
    if "company_count" in col_names:
        return jsonify({"status": "Success", "answer": f"Numero di societa: {rows[0][0] if rows else 0}"})
    if "total_raised" in col_names and "group_key" in col_names and rows:
        items = []
        for r in rows[:5]:
            if not r or r[0] in (None, "") or r[1] is None:
                continue
            try:
                amt = float(r[1])
                items.append(f"{r[0]} ({amt:.2f}M)")
            except Exception:
                items.append(f"{r[0]} ({r[1]}M)")
        return jsonify({"status": "Success", "answer": ("Top: " + ", ".join(items)) if items else "Nessun risultato con importo disponibile."})
    if "total_raised" in col_names:
        val = rows[0][0] if rows else 0
        try:
            val = f"{float(val):.2f}"
        except Exception:
            pass
        return jsonify({"status": "Success", "answer": f"Totale raccolto: {val}M"})

    return jsonify({"status": "Success", "answer": "Nessun risultato."})
