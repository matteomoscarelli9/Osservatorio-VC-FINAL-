#!/usr/bin/env python3
import os
import sqlite3
import subprocess
from datetime import datetime
from pathlib import Path
from flask import Flask, jsonify, request
from openai import OpenAI
import json
import re
import difflib

AUTOMATION_SCRIPT = "/Users/matteomoscarelli/Documents/New project/automations/dealflowit_to_excel.py"
SYNC_SCRIPT = "/Users/matteomoscarelli/Documents/New project/automations/sync_excel_to_db.py"
EXCEL_PATH = "/Users/matteomoscarelli/Library/CloudStorage/OneDrive-Raccoltecondivise-UnitedVentures/United Ventures - United Ventures/04. Portfolio/Portfolio Team/100. OLD/07. Dealflow Meeting MM/Osservatorio VC Italy _.xlsx"
SHEET_NAME = "Funding rounds (3)"
DB_PATH = "/Users/matteomoscarelli/Documents/New project/db/rounds.db"

app = Flask(__name__)

@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

@app.route("/api/chat", methods=["OPTIONS"])
def chat_options():
    return ("", 204)

def sync_db():
    cmd = [
        "python3",
        SYNC_SCRIPT,
        "--path",
        EXCEL_PATH,
        "--sheet",
        SHEET_NAME,
        "--db",
        DB_PATH,
    ]
    return subprocess.run(cmd, capture_output=True, text=True)

def db_conn():
    return sqlite3.connect(DB_PATH)

def ensure_db():
    if not os.path.exists(DB_PATH):
        sync_db()
        return
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='rounds'")
    exists = cur.fetchone() is not None
    conn.close()
    if not exists:
        sync_db()

@app.route("/api/run", methods=["POST"])
def run_job():
    env = os.environ.copy()
    if not env.get("OPENAI_API_KEY"):
        return jsonify({"status": "Error", "error": "OPENAI_API_KEY missing"}), 400

    cmd = [
        "python3",
        AUTOMATION_SCRIPT,
        "--path",
        EXCEL_PATH,
        "--sheet",
        SHEET_NAME,
        "--after",
        datetime.now().strftime("%Y-%m-%d"),
        "--before",
        datetime.now().strftime("%Y-%m-%d"),
        "--recent-days",
        "30"
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if proc.returncode != 0:
        err = proc.stderr.strip() or proc.stdout.strip()
        print(f"[run] error: {err}", flush=True)
        return jsonify({"status": "Error", "error": err}), 500

    rows = "—"
    companies = []
    for line in proc.stdout.splitlines():
        if "Inserted" in line:
            try:
                rows = int(line.split("Inserted")[1].strip().split()[0])
            except Exception:
                rows = "—"
        if line.startswith("RESULT_JSON:"):
            try:
                payload = line.replace("RESULT_JSON:", "", 1)
                data = json.loads(payload)
                companies = data.get("companies", [])
                rows = data.get("rows", rows)
            except Exception:
                pass

    # After run, sync DB
    sync_proc = sync_db()
    if sync_proc.returncode != 0:
        err = sync_proc.stderr.strip() or sync_proc.stdout.strip()
        print(f"[sync] error: {err}", flush=True)
        return jsonify({"status": "Error", "error": err}), 500

    return jsonify({
        "status": "Success",
        "rows": rows,
        "companies": companies,
        "time": datetime.now().strftime("%d %b %Y · %H:%M")
    })

@app.route("/api/sync", methods=["POST"])
def sync_endpoint():
    proc = sync_db()
    if proc.returncode != 0:
        return jsonify({"status": "Error", "error": proc.stderr.strip() or proc.stdout.strip()}), 500
    return jsonify({"status": "Success", "message": proc.stdout.strip()})

@app.route("/api/schema", methods=["GET"])
def schema():
    ensure_db()
    conn = db_conn()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(rounds)")
    cols = [row[1] for row in cur.fetchall()]
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

    if search:
        cur.execute("PRAGMA table_info(rounds)")
        cols = [row[1] for row in cur.fetchall()]
        like_clause = " OR ".join([f'\"{c}\" LIKE ?' for c in cols])
        params = [f"%{search}%"] * len(cols)
        query = f"SELECT * FROM rounds WHERE {like_clause} ORDER BY id DESC LIMIT ? OFFSET ?"
        cur.execute(query, params + [limit, offset])
    else:
        cur.execute("SELECT * FROM rounds ORDER BY id DESC LIMIT ? OFFSET ?", (limit, offset))

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
    cur.execute("PRAGMA table_info(rounds)")
    cols = [row[1] for row in cur.fetchall()]

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
                where_clauses.append(f"{amount_expr} < ?")
                params.append(float(token.split(":", 1)[1]))
            elif token.startswith("gt:"):
                where_clauses.append(f"{amount_expr} > ?")
                params.append(float(token.split(":", 1)[1]))
            elif token.startswith("between:"):
                parts = token.split(":")
                if len(parts) == 3:
                    where_clauses.append(f"{amount_expr} BETWEEN ? AND ?")
                    params.extend([float(parts[1]), float(parts[2])])
            else:
                where_clauses.append(f"{amount_expr} = ?")
                params.append(float(token))
        else:
            where_clauses.append(f"LOWER(\"{col}\") LIKE LOWER(?)")
            params.append(f"%{value}%")

    where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    query = f"SELECT * FROM rounds {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?"
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
    cur.execute("PRAGMA table_info(rounds)")
    cols = [row[1] for row in cur.fetchall()]
    if col not in cols:
        conn.close()
        return jsonify({"status": "Error", "error": "Invalid col"}), 400

    cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
    values = [row[0] for row in cur.fetchall() if row[0] not in (None, "")]
    conn.close()
    return jsonify({"column": col, "values": values})

@app.route("/api/chat", methods=["POST"])
def chat():
    ensure_db()
    payload = request.get_json(force=True)
    question = payload.get("question", "").strip()
    lang = (payload.get("lang") or "it").lower()
    if not question:
        return jsonify({"status": "Error", "error": "Missing question"}), 400
    print(f"[chat] question={question}", flush=True)

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        return jsonify({"status": "Error", "error": "OPENAI_API_KEY missing"}), 400

    conn = db_conn()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(rounds)")
    cols = [row[1] for row in cur.fetchall()]

    client = OpenAI(api_key=api_key, timeout=20.0)
    amount_expr = 'COALESCE(CAST(REPLACE("Round size (€M)", ",", ".") AS REAL), 0)'

    def distinct(col):
        cur.execute(f'SELECT DISTINCT "{col}" FROM rounds')
        return [r[0] for r in cur.fetchall() if r[0]]

    companies = distinct("Company")
    sectors = distinct("Sector 1")
    cities = distinct("HQ")
    leads = distinct("Lead")

    def normalize_text(val: str) -> str:
        return re.sub(r"[^a-z0-9\s]", " ", val.lower()).strip()

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

    # Ask LLM for intent JSON
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
        intent = None

    if not intent:
        intent = {
            "metric": "count",
            "subject": "rounds",
            "group_by": None,
            "top_n": None,
            "filters": {}
        }

    metric = intent.get("metric")
    subject = intent.get("subject")
    group_by = intent.get("group_by") or None
    top_n = intent.get("top_n")
    filters = intent.get("filters") or {}

    # Normalize filters against DB values
    filters["company"] = map_entity(filters.get("company"), companies)
    filters["sector"] = map_entity(filters.get("sector"), sectors)
    filters["city"] = map_entity(filters.get("city"), cities)
    filters["lead"] = map_entity(filters.get("lead"), leads)

    def build_where():
        clauses = []
        params = []
        if filters.get("company"):
            clauses.append('LOWER("Company") LIKE LOWER(?)')
            params.append(f'%{filters["company"]}%')
        if filters.get("sector"):
            clauses.append('LOWER("Sector 1") = LOWER(?)')
            params.append(filters["sector"])
        if filters.get("city"):
            clauses.append('LOWER("HQ") = LOWER(?)')
            params.append(filters["city"])
        if filters.get("lead"):
            clauses.append('LOWER("Lead") = LOWER(?)')
            params.append(filters["lead"])
        if filters.get("year_from") and filters.get("year_to"):
            clauses.append('CAST(substr("Date", -4) AS INT) BETWEEN ? AND ?')
            params.extend([int(filters["year_from"]), int(filters["year_to"])])
        elif filters.get("year_from"):
            clauses.append('CAST(substr("Date", -4) AS INT) >= ?')
            params.append(int(filters["year_from"]))
        elif filters.get("year_eq"):
            clauses.append('"Date" LIKE ?')
            params.append(f'%{filters["year_eq"]}%')
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return where_sql, params

    group_map = {
        "company": '"Company"',
        "city": '"HQ"',
        "sector": '"Sector 1"',
        "year": 'substr("Date", -4)',
        "lead": '"Lead"',
        "quarter": '"Q"',
    }

    where_sql, params = build_where()

    # Build SQL
    if subject == "companies" and metric == "count":
        if filters.get("min_amount"):
            sql = (
                f'SELECT COUNT(*) AS company_count FROM ('
                f'SELECT "Company", SUM({amount_expr}) AS total_raised '
                f'FROM rounds {where_sql} GROUP BY "Company" '
                f'HAVING total_raised > ?)'
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

    if not sql.lower().strip().startswith("select"):
        return jsonify({"status": "Error", "error": "Query rejected"}), 400

    try:
        cur.execute(sql, params)
        rows = cur.fetchall()
        col_names = [description[0] for description in cur.description]
    except Exception as e:
        conn.close()
        return jsonify({"status": "Error", "error": f"SQL error: {e}"}), 500
    conn.close()

    data = [dict(zip(col_names, row)) for row in rows]

    # Deterministic answer generation
    def period_text():
        if filters.get("year_from") and filters.get("year_to"):
            return f"dal {filters['year_from']} al {filters['year_to']}"
        if filters.get("year_from"):
            return f"dal {filters['year_from']} ad oggi"
        if filters.get("year_eq"):
            return f"nel {filters['year_eq']}"
        return ""

    if "round_count" in col_names:
        count = rows[0][0] if rows else 0
        period = period_text()
        if filters.get("company"):
            answer = f"{filters['company']} ha fatto {count} round {period}.".strip()
        else:
            answer = f"Totale round {period}: {count}.".strip()
        return jsonify({"status": "Success", "answer": answer, "rows": data, "columns": col_names})
    if "company_count" in col_names:
        count = rows[0][0] if rows else 0
        period = period_text()
        min_amount = filters.get("min_amount")
        if min_amount:
            answer = f"Società con raccolto > {min_amount}M {period}: {count}."
        else:
            answer = f"Numero di società {period}: {count}."
        return jsonify({"status": "Success", "answer": answer, "rows": data, "columns": col_names})
    if "total_raised" in col_names and "group_key" in col_names and rows:
        top = rows[0]
        label = group_by or "gruppo"
        answer = f"{label.title()} con valore più alto: {top[0]} ({top[1]}M)."
        return jsonify({"status": "Success", "answer": answer, "rows": data, "columns": col_names})
    if "total_raised" in col_names:
        total = rows[0][0] if rows else 0
        period = period_text()
        answer = f"Totale raccolto {period}: {total}M.".strip()
        return jsonify({"status": "Success", "answer": answer, "rows": data, "columns": col_names})

    # Second pass: generate a natural language answer from results
    result_lines = [",".join([c.replace(",", " ") for c in col_names])]
    for r in rows[:200]:
        result_lines.append(",".join([str(x).replace(",", " ") for x in r]))
    result_blob = "\n".join(result_lines)

    summarizer_system = (
        "You are ChatGPT answering questions about Italian startup funding rounds. "
        "Use only the provided result data. Answer in Italian, concise and clear. "
        "If the result is empty, say so."
    )
    summarizer_user = f"Question: {question}\nResults (CSV):\n{result_blob}"
    resp2 = client.responses.create(
        model="gpt-4.1-mini",
        input=[{"role": "system", "content": summarizer_system}, {"role": "user", "content": summarizer_user}],
    )
    answer = resp2.output_text if hasattr(resp2, "output_text") else ""
    print("[chat] answered via LLM", flush=True)
    return jsonify({"status": "Success", "answer": answer, "rows": data, "columns": col_names})

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5174, debug=True)
