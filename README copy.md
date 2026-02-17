# TWIS Observatory Monorepo

Single Git repository with:
- `frontend/`: React + Vite UI
- `api/`: Python (Flask) serverless API for Vercel
- `automations/`: local scripts (Outlook + Excel + DB sync)
- `db/`: local SQLite database (ignored from Git)

## Repository Structure

```text
api/
  index.py
automations/
  dealflowit_to_excel.py
  sync_excel_to_db.py
  hq_cache.json
frontend/
  src/
  package.json
  vite.config.js
requirements.txt
vercel.json
```

## Local Run

Backend:

```bash
python3 "/Users/matteomoscarelli/Documents/New project/frontend/server.py"
```

Frontend:

```bash
cd "/Users/matteomoscarelli/Documents/New project/frontend"
npm run dev
```

## Vercel Deployment (Same Repo, Two Projects)

Use two Vercel projects connected to this same Git repo:

1. Backend project (`twis-backend`)
   - Root directory: repo root
   - Uses `vercel.json` + `api/index.py`
   - Env vars:
     - `OPENAI_API_KEY`
     - `DB_PATH` (optional; default expects `db/rounds.db` in repo runtime)

2. Frontend project (`twis-frontend`)
   - Root directory: `frontend`
   - Framework: Vite
   - Build command: `npm run build`
   - Output directory: `dist`
   - Env vars:
     - `VITE_API_BASE_URL=https://<your-backend-vercel-domain>`

## Important Notes

- Endpoints `/api/run` and `/api/sync` are not supported on Vercel backend (they require local Outlook/Excel automation).
- Keep automation scripts local or move them to a dedicated worker environment.
- `db/*.db` is ignored by default; do not commit sensitive data.

## Git Push Checklist

```bash
cd "/Users/matteomoscarelli/Documents/New project"
git add .
git status
git commit -m "Prepare monorepo for Vercel frontend+backend deployment"
git push
```
