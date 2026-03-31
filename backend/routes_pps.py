"""
PPS (Personalized Philanthropy Statement) routes.

Two endpoints:
  POST /api/pps/generate — calls Claude Sonnet to draft the PPS from the donor
                           profile. Can be called mid-interview (after Q7) or on
                           completion. Result stored in profile_data["outputs"].
  POST /api/pps/save     — donor edits the draft and hits Save; stores final text.

The question_bank note says "always generate PPS in background even if donor
skips" — the frontend should fire /generate silently after Q8 regardless of
whether the donor clicked 'Produce my PPS' or 'Skip'.
"""
from typing import Annotated

import anthropic
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from database import get_db
from config import PPS_MODEL, PPS_MAX_TOKENS, SESSION_ID_PATTERN, ANTHROPIC_API_KEY
from limiter import limiter
from db_helpers import get_or_404, save_profile

DB = Annotated[Session, Depends(get_db)]
router = APIRouter(prefix="/api/pps", tags=["pps"])


# ── Schemas ───────────────────────────────────────────────────────────────────

class GenerateRequest(BaseModel):
    session_id: str = Field(..., pattern=SESSION_ID_PATTERN)

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
            }
        }
    }


class GenerateResponse(BaseModel):
    session_id: str
    pps_text: str


class SaveRequest(BaseModel):
    session_id: str = Field(..., pattern=SESSION_ID_PATTERN)
    pps_text: str = Field(..., min_length=1, max_length=5000)

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "pps_text": "I believe in empowering underserved communities through education and economic opportunity...",
            }
        }
    }


class SaveResponse(BaseModel):
    session_id: str
    saved: bool


# ── Label lookup tables (module-level to avoid re-creating on every call) ─────

_NOT_SPECIFIED = "not specified"

_VALUE_LABELS = {
    "results_driven":      "driven by measurable results",
    "values_guided":       "guided by personal values",
    "personal_connection": "motivated by personal connection",
    "curious_exploratory": "curious and exploratory",
    "strategic_impact":    "strategic about long-term impact",
    "emotionally_moved":   "emotionally moved by stories",
    "time_constrained":    "thoughtful but time-constrained",
    "legacy":              "focused on philanthropic legacy",
    "figuring_it_out":     "still exploring their giving approach",
}

_ENGAGEMENT_LABELS = {
    "give_only":        "give only (hands-off)",
    "receive_updates":  "receive updates (light touch)",
    "attend_events":    "attend occasional events",
    "volunteer":        "volunteer occasionally",
    "expertise":        "use professional expertise",
    "deep_involvement": "deep involvement (board/committee level)",
}

_MANDATORY_LABELS = {
    "none":               "no mandatory requirements",
    "volunteer_only":     "must have volunteer opportunities",
    "events_only":        "must have events or webinars",
    "measurable_updates": "must provide regular measurable updates",
    "expertise_only":     "must be able to use professional expertise",
}

_BUDGET_LABELS = {
    "no_say":    "prefers not to share budget",
    "under_1k":  "under $1,000",
    "1k_5k":     "$1,000–$5,000",
    "5k_25k":    "$5,000–$25,000",
    "25k_100k":  "$25,000–$100,000",
    "100k_plus": "$100,000+",
}



# ── Helpers ───────────────────────────────────────────────────────────────────

def _format_categories(q3: list, per_cat: dict) -> str:
    cats = q3 if isinstance(q3, list) else [q3]
    lines = []
    for cat in cats:
        cd = per_cat.get(cat, {})
        geo = cd.get("Q4a_geography_scope", "no geographic preference")
        pops = cd.get("Q4b_target_population") or []
        pops_str = ", ".join(pops) if pops else "no specific population preference"
        lines.append(f"  • {cat}: geography = {geo}; target population = {pops_str}")
    return "\n".join(lines) if lines else "  • Not specified"


def _format_engagement(q5) -> str:
    if isinstance(q5, list):
        eng_list = q5
    elif q5:
        eng_list = [q5]
    else:
        eng_list = []
    return ", ".join(_ENGAGEMENT_LABELS.get(e, e) for e in eng_list) or _NOT_SPECIFIED


# ── Prompt builder ────────────────────────────────────────────────────────────

def _build_prompt(firstname: str | None, interview: dict, per_cat: dict) -> str:
    name = firstname or "the donor"

    q1 = interview.get("Q1_initial_preferences", "").strip()
    q2 = interview.get("Q2_values") or []
    q3 = interview.get("Q3_cause_categories") or []
    q5 = interview.get("Q5_engagement_level") or []
    q6 = interview.get("Q6_mandatory_engagement", "none")
    q7 = interview.get("Q7_budget_12mo")

    q2_list = q2 if isinstance(q2, list) else [q2]
    values_str = ", ".join(_VALUE_LABELS.get(v, v) for v in q2_list) or _NOT_SPECIFIED
    budget_str = _BUDGET_LABELS.get(q7, q7) if q7 else _NOT_SPECIFIED

    lines = [
        f"Donor name: {name}",
        f"Initial preferences (free text): {q1 or 'none provided'}",
        f"Philanthropic values: {values_str}",
        f"Selected cause areas:\n{_format_categories(q3, per_cat)}",
        f"Preferred engagement style: {_format_engagement(q5)}",
        f"Mandatory engagement requirement: {_MANDATORY_LABELS.get(q6, q6)}",
        f"Annual giving budget: {budget_str}",
    ]

    profile_summary = "\n".join(lines)

    return f"""You are a skilled philanthropic advisor writing a Personalized Philanthropy Statement (PPS) for a donor.

A PPS is a single, well-crafted paragraph (4–6 sentences) that captures the donor's philanthropic identity, values, priorities, and intentions. It reads like a personal mission statement — warm, specific, and written in the first person from the donor's point of view. It should feel personal, not generic.

Here is the donor's profile:

{profile_summary}

Write the PPS now. Output ONLY the paragraph — no title, no preamble, no commentary."""


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post(
    "/generate",
    response_model=GenerateResponse,
    summary="Generate a Personalized Philanthropy Statement",
    description=(
        "Calls Claude Sonnet to draft a 4–6 sentence first-person philanthropy statement "
        "based on the donor's full interview profile (values, cause areas, geography, "
        "population preferences, engagement style, and budget). "
        "The draft is stored as `pps_generated` in the donor profile. "
        "Call this after Q7 (or whenever Q8 is reached) — even if the donor skips the PPS step. "
        "Rate limited to 5 requests/minute per IP."
    ),
    responses={
        404: {"description": "Session not found"},
        429: {"description": "Rate limit exceeded"},
        500: {"description": "ANTHROPIC_API_KEY not configured"},
    },
)
@limiter.limit("5/minute")
def generate_pps(request: Request, req: GenerateRequest, db: DB):  # `request` required by slowapi
    """Generate a PPS draft using Claude Sonnet and store it in the donor profile."""
    donor = get_or_404(db, req.session_id, lock=True)

    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not configured")

    interview = donor.profile_data.get("interview", {})
    per_cat   = donor.profile_data.get("per_category", {})

    prompt = _build_prompt(donor.firstname, interview, per_cat)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY, timeout=30.0)
    msg = client.messages.create(
        model=PPS_MODEL,
        max_tokens=PPS_MAX_TOKENS,
        messages=[{"role": "user", "content": prompt}],
    )
    pps_text = msg.content[0].text.strip()

    # Persist draft — separate from pps_text (the donor-edited final version)
    data = {**donor.profile_data}
    outputs = {**data.get("outputs", {})}
    outputs["pps_generated"] = pps_text
    data["outputs"] = outputs
    donor.profile_data = data
    save_profile(db, donor)

    return GenerateResponse(session_id=req.session_id, pps_text=pps_text)


@router.post(
    "/save",
    response_model=SaveResponse,
    summary="Save the donor-edited PPS as the final version",
    description=(
        "Stores the donor's edited philanthropy statement as `pps_text` in their profile. "
        "This is separate from `pps_generated` (the AI draft) so both versions are preserved. "
        "Maximum 5,000 characters."
    ),
    responses={
        404: {"description": "Session not found"},
        400: {"description": "pps_text is empty or exceeds 5,000 characters"},
    },
)
def save_pps(req: SaveRequest, db: DB):
    """Save the donor-edited PPS as the final version."""
    donor = get_or_404(db, req.session_id, lock=True)

    data = {**donor.profile_data}
    outputs = {**data.get("outputs", {})}
    outputs["pps_text"] = req.pps_text.strip()
    data["outputs"] = outputs
    donor.profile_data = data
    save_profile(db, donor)

    return SaveResponse(session_id=req.session_id, saved=True)


@router.get(
    "/{session_id}",
    summary="Get PPS for a session",
    description=(
        "Returns both the AI-generated draft (`pps_generated`) and the donor-edited "
        "final version (`pps_text`). Either may be `null` if not yet produced."
    ),
    responses={404: {"description": "Session not found"}},
)
def get_pps(session_id: str, db: DB):
    """Return the current PPS for a session (generated draft + saved final if any)."""
    donor = get_or_404(db, session_id)

    outputs = donor.profile_data.get("outputs", {})
    return {
        "session_id":    session_id,
        "pps_generated": outputs.get("pps_generated"),  # Sonnet draft
        "pps_text":      outputs.get("pps_text"),        # donor-edited final
    }
