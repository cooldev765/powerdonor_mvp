"""
PowerDonor.AI — FastAPI application entry point.
All routes are in separate modules; main.py only wires them together.
"""
import os
import time
import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from database import engine, Base
from models import Donor          # ensures Donor table is created on startup
from config import ALLOWED_ORIGINS
from limiter import limiter
from routes_tts import router as tts_router
from routes_interview import router as interview_router
from routes_pps import router as pps_router
from routes_matching import router as matching_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("powerdonor")

# ── App ───────────────────────────────────────────────────────────────────────
_ENV = os.getenv("ENVIRONMENT", "development")

app = FastAPI(
    title="PowerDonor.AI",
    version="2.0.0",
    docs_url="/docs" if _ENV != "production" else None,
    redoc_url="/redoc" if _ENV != "production" else None,
)

# ── Rate limiter ──────────────────────────────────────────────────────────────
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ── CORS ──────────────────────────────────────────────────────────────────────
# In production set ENVIRONMENT=production and ALLOWED_ORIGINS explicitly.
# In development allow everything so local file:// and any localhost port works.
_cors_origins = ["*"] if _ENV != "production" else ALLOWED_ORIGINS
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=_ENV == "production",
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request logging ───────────────────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.time()
    response = await call_next(request)
    ms = round((time.time() - start) * 1000)
    logger.info("%s %s %s %dms", request.method, request.url.path, response.status_code, ms)
    return response


# ── Startup checks ────────────────────────────────────────────────────────────
@app.on_event("startup")
def check_env():
    """Warn loudly at startup if required env vars are missing."""
    required = ["DATABASE_URL", "ANTHROPIC_API_KEY", "ELEVENLABS_API_KEY"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        logger.warning("MISSING ENV VARS — some features will fail: %s", ", ".join(missing))
    Base.metadata.create_all(bind=engine, checkfirst=True)


# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(tts_router)
app.include_router(interview_router)
app.include_router(pps_router)
app.include_router(matching_router)


@app.get("/health", summary="Health check")
def health():
    return {"status": "ok", "service": "PowerDonor.AI backend v2", "environment": _ENV}
