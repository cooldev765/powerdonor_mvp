# PowerDonor.AI

AI-powered donor matching platform. Dafney (our voice AI) interviews donors about their giving preferences and matches them with nonprofits from a database of 220,000+ organizations.

## Live Backend

```
https://backend-production-fc024.up.railway.app
```

- **API Docs:** https://backend-production-fc024.up.railway.app/docs
- **Health:** https://backend-production-fc024.up.railway.app/health

## Repo Structure

```
powerdonor_mvp/
├── backend/          # FastAPI backend — deployed to Railway
├── frontend/         # React frontend — build here
├── new_scripts/      # Data pipeline scripts
├── magid_scripts/    # Enrichment scripts (already run)
├── sql scripts/      # One-time DB modifications (already applied)
└── demo.html         # Vanilla JS reference implementation of full interview flow
```

## API Overview

### Interview
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/interview/start` | Start session, returns `session_id` + first question |
| POST | `/api/interview/answer` | Submit answer, returns next question |
| GET | `/api/interview/session/{session_id}` | Get full donor profile |
| GET | `/api/interview/question/{question_id}` | Fetch a single question (debugging) |

### TTS
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/tts` | Send text, returns MP3 audio stream |

### PPS (Philanthropy Statement)
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/pps/generate` | Generate AI-written donor statement |
| POST | `/api/pps/save` | Save edited statement |
| GET | `/api/pps/{session_id}` | Get saved statement |

### Matching
| Method | Endpoint | Description |
|---|---|---|
| POST | `/api/matching/find` | **Not built yet** — returns 501 |
| GET | `/api/matching/results/{session_id}` | Get saved match results |

## Interview Flow

10-question flow driven entirely by the backend. Frontend renders what it receives — no flow logic on the client.

```
Q1 (free text) → Q2 (values) → Q3 (categories) →
  [Q4a (geography) → Q4b (population) → Q4c (subcategories)] × each category →
Q5 → Q6 → Q7 → Q8 (PPS) → Q9 → Q10 → complete
```

### Question Types
| Type | UI |
|---|---|
| `free_text` | textarea |
| `single_select` | radio buttons |
| `multi_select` | checkboxes |
| `multi_select_dynamic` | checkboxes, options injected by backend (Q4c only) |
| `multi_select_grouped` | grouped checkboxes |
| `multi_select_with_text` | checkboxes, some trigger a text input |
| `single_select_with_numeric` | radio, one option triggers number input |
| `pps_generation` | trigger PPS generation flow |

See `demo.html` for a working reference implementation of all question types.

## Frontend Setup

Point your app at the live backend:

```
VITE_API_URL=https://backend-production-fc024.up.railway.app
```

Do **not** push directly to `main` — the backend is live and any push triggers a redeploy. Work on a feature branch and open a PR.

## Running the Backend Locally

```bash
cd backend
cp ../.env.example .env   # fill in your keys
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

Required env vars:
```
DATABASE_URL=
ANTHROPIC_API_KEY=
ELEVENLABS_API_KEY=
ELEVENLABS_VOICE_ID=
```

## Scripts

### `magid_scripts/` — Enrichment Pipeline (already run)
Scripts used to build and enrich the `mvp_charities` database. Use as reference or re-run if needed. Requires `ANTHROPIC_API_KEY` and `DATABASE_URL`.

| File | Description |
|---|---|
| `ingest_irs_data.py` | Load raw IRS 990 data into DB |
| `discover_urls.py` | Find websites for orgs |
| `pipeline_step1/2/3_crawl_round2.py` | Crawl websites → enrich with Haiku → load results |
| `haiku_pipeline.py` | Full enrichment pipeline (mission, programs, keywords, impact metrics) |
| `haiku_verify.py` | Verify enrichment quality |
| `Extract_FIN_IRS.py` | Extract financials from IRS forms |

### `new_scripts/` — Data Pipeline
| File | Description |
|---|---|
| `pipeline_subcategory.py` | Classify orgs into subcategories using NTEE + keywords. Commands: `run`, `rerun`, `quality` |

### `sql scripts/` — DB Modifications
One-time SQL scripts already applied to the database. Kept for reference only.
