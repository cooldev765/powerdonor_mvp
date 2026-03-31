"""
Matching routes — vector-based recommendation engine.

TODO: implement pipeline
"""
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from limiter import limiter

from database import get_db
from config import DEFAULT_RESULTS_PER_CATEGORY, SESSION_ID_PATTERN
from db_helpers import get_or_404

logger = logging.getLogger("powerdonor")

DB = Annotated[Session, Depends(get_db)]
router = APIRouter(prefix="/api/matching", tags=["matching"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class MatchRequest(BaseModel):
    session_id: str = Field(..., pattern=SESSION_ID_PATTERN)
    results_per_category: int = Field(
        default=DEFAULT_RESULTS_PER_CATEGORY,
        ge=1,
        le=20,
        description="Number of charity results to return per cause category (1–20).",
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "results_per_category": 3,
            }
        }
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post("/find", summary="Find charity matches")
@limiter.limit("10/minute")
def find_matches(request: Request, req: MatchRequest, db: DB):  # noqa: ARG001
    raise NotImplementedError("Matching pipeline not yet implemented")


@router.get("/results/{session_id}", summary="Get previously computed match results")
def get_results(session_id: str, db: DB):
    donor = get_or_404(db, session_id)
    return {"session_id": session_id, "results": donor.profile_data.get("match_results", {})}
