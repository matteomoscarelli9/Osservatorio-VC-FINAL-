import json
import os
import re
import sqlite3
import difflib
import subprocess
import importlib.util
import inspect
import hashlib
from datetime import datetime
from pathlib import Path

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


@app.route("/api/run", methods=["POST"])
def run_job():
    payload = request.get_json(silent=True) or {}
    use_current = bool(payload.get("use_current", False))
    subject = str(payload.get("subject", "TWIS")).strip() or "TWIS"
    sender = str(payload.get("sender", "")).strip()
    rss_url = str(payload.get("rss_url", "")).strip()
    recent_days = int(payload.get("recent_days", 30))
    debug = bool(payload.get("debug", False))

    if not os.path.exists(AUTOMATION_SCRIPT):
        return jsonify({"status": "Error", "error": f"Automation script not found: {AUTOMATION_SCRIPT}"}), 500

    cmd = [
        "python3",
        AUTOMATION_SCRIPT,
        "--model",
        EXTRACTION_MODEL,
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
    # Emit script output to runtime logs for easier debugging from Vercel.
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

    if search:
        placeholder = ph()
        like_clause = " OR ".join([f'"{c}" LIKE {placeholder}' for c in cols])
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
    return jsonify({"rows": data, "columns": col_names})


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
    amount_expr = 'COALESCE(CAST(REPLACE("Round size (€M)", ",", ".") AS REAL), 0)'
    for col, value in filters.items():
        if col not in cols:
            continue
        if value is None or str(value).strip() == "":
            continue
        if col == "Round size (€M)":
            token = str(value)
            if token.startswith("lt:"):
                where_clauses.append(f"{amount_expr} < {placeholder}")
                params.append(float(token.split(":", 1)[1]))
            elif token.startswith("gt:"):
                where_clauses.append(f"{amount_expr} > {placeholder}")
                params.append(float(token.split(":", 1)[1]))
            elif token.startswith("between:"):
                parts = token.split(":")
                if len(parts) == 3:
                    where_clauses.append(f"{amount_expr} BETWEEN {placeholder} AND {placeholder}")
                    params.extend([float(parts[1]), float(parts[2])])
            else:
                where_clauses.append(f"{amount_expr} = {placeholder}")
                params.append(float(token))
        else:
            where_clauses.append(f'LOWER("{col}") LIKE LOWER({placeholder})')
            params.append(f"%{value}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM rounds {where_sql} ORDER BY id DESC LIMIT {placeholder} OFFSET {placeholder}"
    cur.execute(query, params + [limit, offset])
    rows = cur.fetchall()
    col_names = [description[0] for description in cur.description]
    conn.close()
    data = [dict(zip(col_names, row)) for row in rows]
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
        cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
        values = [row[0] for row in cur.fetchall() if row[0] not in (None, "")]
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
    amount_expr = 'COALESCE(CAST(REPLACE("Round size (€M)", ",", ".") AS REAL), 0)'

    def distinct(col):
        cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
        return [r[0] for r in cur.fetchall() if r[0]]

    companies = distinct("Company")
    sectors = distinct("Sector 1")
    cities = distinct("HQ")
    leads = distinct("Lead")

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
            "Extract intent to query a SQLite database of Italian funding rounds. "
            "Return JSON only with keys: metric, subject, group_by, top_n, filters. "
            "metric: count|sum|avg|max|min. subject: rounds|amount|companies. "
            "group_by: company|city|sector|year|lead|quarter|null. "
            "filters may include: year_eq, year_from, year_to, company, city, sector, lead, min_amount."
        )
        user = {"question": question}
        resp = client.responses.create(
            model="gpt-4.1-mini",
            input=[{"role": "system", "content": system}, {"role": "user", "content": json.dumps(user, ensure_ascii=False)}],
        )
        raw = resp.output_text if hasattr(resp, "output_text") else ""
        intent = json.loads(raw)
    except Exception:
        intent = {"metric": "count", "subject": "rounds", "group_by": None, "top_n": None, "filters": {}}

    metric = intent.get("metric")
    subject = intent.get("subject")
    group_by = intent.get("group_by") or None
    top_n = intent.get("top_n")
    filters = intent.get("filters") or {}

    filters["company"] = map_entity(filters.get("company"), companies)
    filters["sector"] = map_entity(filters.get("sector"), sectors)
    filters["city"] = map_entity(filters.get("city"), cities)
    filters["lead"] = map_entity(filters.get("lead"), leads)

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
        if filters.get("min_amount"):
            sql = (
                f'SELECT COUNT(*) AS company_count FROM ('
                f'SELECT "Company", SUM({amount_expr}) AS total_raised '
                f'FROM rounds {where_sql} GROUP BY "Company" '
                f'HAVING total_raised > {placeholder})'
            )
            params.append(float(filters["min_amount"]))
        else:
            sql = f'SELECT COUNT(DISTINCT "Company") AS company_count FROM rounds {where_sql}'
    elif subject == "rounds" and metric == "count":
        if group_by in group_map:
            group_col = group_map[group_by]
            sql = f'SELECT {group_col} AS group_key, COUNT(*) AS round_count FROM rounds {where_sql} GROUP BY {group_col} ORDER BY round_count DESC LIMIT {top_n or 200}'
        else:
            sql = f'SELECT COUNT(*) AS round_count FROM rounds {where_sql}'
    elif subject == "amount":
        agg = "SUM" if metric in ("sum", "max") else "AVG"
        if group_by in group_map:
            group_col = group_map[group_by]
            sql = f'SELECT {group_col} AS group_key, {agg}({amount_expr}) AS total_raised FROM rounds {where_sql} GROUP BY {group_col} ORDER BY total_raised DESC LIMIT {top_n or 200}'
        else:
            sql = f'SELECT {agg}({amount_expr}) AS total_raised FROM rounds {where_sql}'
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

    if "round_count" in col_names:
        return jsonify({"status": "Success", "answer": f"Totale round: {rows[0][0] if rows else 0}"})
    if "company_count" in col_names:
        return jsonify({"status": "Success", "answer": f"Numero di societa: {rows[0][0] if rows else 0}"})
    if "total_raised" in col_names and "group_key" in col_names and rows:
        return jsonify({"status": "Success", "answer": f"Top: {rows[0][0]} ({rows[0][1]}M)"})
    if "total_raised" in col_names:
        return jsonify({"status": "Success", "answer": f"Totale raccolto: {rows[0][0] if rows else 0}M"})

    return jsonify({"status": "Success", "answer": "Nessun risultato."})
