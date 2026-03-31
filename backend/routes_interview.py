"""
Interview routes — backend-driven state machine.

The backend owns the flow. After every answer it computes the next question
and returns it fully hydrated from question_bank.json. The frontend only
renders what it receives — no flow logic on the client.

Flow:
  Q1 → Q2 → Q3 → [Q4a → Q4b → Q4c] × each selected category → Q5 → Q6 → Q7 → Q8 → Q9 → Q10 → complete
"""
import uuid
import json
import os
from typing import Annotated, Any
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, EmailStr, Field, field_validator
from sqlalchemy.orm import Session

from database import get_db
from models import Donor
from config import SESSION_ID_PATTERN, MAX_CATEGORIES, MAX_ANSWER_LIST_ITEMS, MAX_DICT_KEYS, MAX_DICT_DEPTH, MAX_DICT_BYTES, CATEGORY_TO_COL
from db_helpers import get_or_404, save_profile

# ── Col key → display name (Q3 option IDs are col keys; loop needs display names) ──
_COL_TO_CATEGORY = {v: k for k, v in CATEGORY_TO_COL.items()}

# ── Question bank ─────────────────────────────────────────────────────────────
_QB_PATH = os.path.join(os.path.dirname(__file__), "question_bank.json")
_QB_CACHE: dict = {}
_QB_MTIME: float = 0.0


def _load_qb() -> dict:
    """Return question bank, reloading from disk if the file has changed."""
    global _QB_CACHE, _QB_MTIME
    mtime = os.path.getmtime(_QB_PATH)
    if mtime != _QB_MTIME:
        with open(_QB_PATH) as f:
            _QB_CACHE = json.load(f)
        _QB_MTIME = mtime
    return _QB_CACHE


# ── Taxonomy (subcategory options per category) ───────────────────────────────
_TAXONOMY_PATH = os.path.join(os.path.dirname(__file__), "taxonomy.json")
with open(_TAXONOMY_PATH) as _f:
    _TAXONOMY: dict = json.load(_f)

# Linear question order (category loop is handled separately)
FLOW = [
    "Q1_initial_preferences",
    "Q2_values",
    "Q3_cause_categories",
    # Q4a + Q4b + Q4c are injected here per selected category (see _next_question)
    "Q5_engagement_level",
    "Q6_mandatory_engagement",
    "Q7_budget_12mo",
    "Q8_pps",
    "Q9_results_per_category",
    "Q10_next_steps",
]

# Questions that repeat once per selected category
CATEGORY_LOOP = ["Q4a_geography_scope", "Q4b_target_population", "Q4c_subcategories"]

DB = Annotated[Session, Depends(get_db)]

router = APIRouter(prefix="/api/interview", tags=["interview"])


# ── Validation helpers ────────────────────────────────────────────────────────

def _validate_dict_depth(d: dict, max_depth: int, max_keys: int, _current: int = 0) -> None:
    if _current > max_depth:
        raise ValueError(f"Dict answer exceeds maximum nesting depth of {max_depth}")
    if len(d) > max_keys:
        raise ValueError(f"Dict answer exceeds {max_keys} keys at one level")
    for v in d.values():
        if isinstance(v, dict):
            _validate_dict_depth(v, max_depth, max_keys, _current + 1)


# ── Schemas ───────────────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    firstname: str = Field(..., min_length=1, max_length=100)
    email: EmailStr

    model_config = {
        "json_schema_extra": {
            "example": {
                "firstname": "Sarah",
                "email": "sarah@example.com",
            }
        }
    }


class StartResponse(BaseModel):
    session_id: str
    next_question: dict


class AnswerRequest(BaseModel):
    session_id: str = Field(..., pattern=SESSION_ID_PATTERN)
    question_id: str = Field(..., min_length=1, max_length=100)
    answer: str | list[str] | dict[str, Any]

    @field_validator("answer")
    @classmethod
    def validate_answer(cls, v):
        if isinstance(v, str):
            if len(v) > 2000:
                raise ValueError("Free text answer exceeds 2000 character limit")
        elif isinstance(v, list):
            if len(v) > MAX_ANSWER_LIST_ITEMS:
                raise ValueError(f"List answer exceeds {MAX_ANSWER_LIST_ITEMS} items")
            if len(v) > MAX_CATEGORIES:
                raise ValueError(f"Cannot select more than {MAX_CATEGORIES} categories")
        elif isinstance(v, dict):
            if len(json.dumps(v, separators=(",", ":"))) > MAX_DICT_BYTES:
                raise ValueError(f"Dict answer exceeds {MAX_DICT_BYTES} byte limit")
            _validate_dict_depth(v, MAX_DICT_DEPTH, MAX_DICT_KEYS)
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "session_id": "550e8400-e29b-41d4-a716-446655440000",
                "question_id": "Q3_cause_categories",
                "answer": ["education", "health"],
            }
        }
    }


class AnswerResponse(BaseModel):
    session_id: str
    saved: bool
    next_question: dict | None = None   # None when interview is complete
    complete: bool = False


class SessionResponse(BaseModel):
    session_id: str
    firstname: str | None
    is_complete: bool
    profile: dict


# ── State machine ─────────────────────────────────────────────────────────────

def _hydrate(question_id: str, category: str | None = None) -> dict:
    """Return the full question object from question_bank.json."""
    q = dict(_load_qb().get(question_id, {}))
    q["id"] = question_id
    if category:
        q["category"] = category
        # Interpolate {category_name} placeholder in display/tts fields
        for field in ("display_text", "tts_prompt", "tts_reprompt"):
            if field in q and "{category_name}" in q[field]:
                q[field] = q[field].replace("{category_name}", category)
        # Inject subcategory options for Q4c from taxonomy
        if question_id == "Q4c_subcategories":
            subcats = _TAXONOMY.get(category, [])
            q["options"] = [{"id": s, "label": s} for s in subcats]
    return q


def _init_state() -> dict:
    return {
        "current_question": "Q1_initial_preferences",
        "current_category": None,
        "categories_pending": [],
        "categories_done": [],
    }


def _next_question(state: dict, answered_id: str, answer: Any) -> tuple[dict | None, dict]:
    """
    Given the just-answered question and answer, compute the next question.
    Returns (next_question_dict_or_None, updated_state).
    None means the interview is complete.
    """
    # Q3 answered — populate the category loop
    if answered_id == "Q3_cause_categories":
        raw = answer if isinstance(answer, list) else [answer]
        # Map col keys (cat_arts) → display names (Arts, Culture & Media)
        categories = [_COL_TO_CATEGORY.get(c, c) for c in raw]
        state["categories_pending"] = list(categories)
        state["categories_done"] = []
        if categories:
            cat = state["categories_pending"][0]
            state["current_category"] = cat
            state["current_question"] = "Q4a_geography_scope"
            return _hydrate("Q4a_geography_scope", cat), state

    # Q4a answered — move to Q4b for same category
    if answered_id == "Q4a_geography_scope":
        cat = state["current_category"]
        state["current_question"] = "Q4b_target_population"
        return _hydrate("Q4b_target_population", cat), state

    # Q4b answered — move to Q4c for same category
    if answered_id == "Q4b_target_population":
        cat = state["current_category"]
        state["current_question"] = "Q4c_subcategories"
        return _hydrate("Q4c_subcategories", cat), state

    # Q4c answered — next category or exit loop
    if answered_id == "Q4c_subcategories":
        state["categories_done"].append(state["current_category"])
        state["categories_pending"].pop(0)

        if state["categories_pending"]:
            cat = state["categories_pending"][0]
            state["current_category"] = cat
            state["current_question"] = "Q4a_geography_scope"
            return _hydrate("Q4a_geography_scope", cat), state

        # Loop done — continue to Q5
        state["current_category"] = None
        state["current_question"] = "Q5_engagement_level"
        return _hydrate("Q5_engagement_level"), state

    # Linear flow
    if answered_id in FLOW:
        idx = FLOW.index(answered_id)
        if idx < len(FLOW) - 1:
            next_id = FLOW[idx + 1]
            state["current_question"] = next_id
            return _hydrate(next_id), state

    # Q10 answered — complete
    return None, state


# ── Helpers ───────────────────────────────────────────────────────────────────

def _store_answer(data: dict, question_id: str, answer: Any, category: str | None) -> dict:
    """Write answer into the correct section of profile_data."""
    if question_id in CATEGORY_LOOP:
        per = {**data.get("per_category", {})}
        cat_data = {**per.get(category, {})}
        cat_data[question_id] = answer
        per[category] = cat_data
        data["per_category"] = per
    else:
        interview = {**data.get("interview", {})}
        interview[question_id] = answer
        data["interview"] = interview
    return data


# ── Routes ────────────────────────────────────────────────────────────────────

@router.post(
    "/start",
    response_model=StartResponse,
    summary="Start a new donor interview",
    description=(
        "Creates a new donor session with a unique `session_id`. "
        "Returns the first interview question fully hydrated from the question bank. "
        "The `session_id` must be passed in every subsequent `/answer` call."
    ),
    responses={
        400: {"description": "Validation error — invalid email or name too long"},
    },
)
def start_interview(req: StartRequest, db: DB):
    """Create a new donor session and return the first question."""
    session_id = str(uuid.uuid4())
    state = _init_state()

    donor = Donor(
        session_id=session_id,
        firstname=req.firstname,
        email=str(req.email).lower(),
        profile_data={
            "interview": {},
            "per_category": {},
            "outputs": {},
            "_state": state,
        },
    )
    db.add(donor)
    db.commit()

    first_question = _hydrate("Q1_initial_preferences")
    # Personalise the first TTS with the donor's name
    first_question["tts_prompt"] = first_question.get("tts_prompt", "").replace(
        "{firstname}", req.firstname
    )

    return StartResponse(session_id=session_id, next_question=first_question)


@router.post(
    "/answer",
    response_model=AnswerResponse,
    summary="Submit an answer and advance the interview",
    description=(
        "Stores the donor's answer for `question_id` and returns the next question. "
        "The `answer` field accepts a string (free text), a list of strings (multi-select), "
        "or a dict (structured answer). "
        "When the interview is finished `complete` is `true` and `next_question` is `null`."
    ),
    responses={
        404: {"description": "Session not found"},
        400: {"description": "Interview already complete or answer too long (>2000 chars)"},
    },
)
def submit_answer(req: AnswerRequest, db: DB):
    """Store an answer and return the next question."""
    donor = get_or_404(db, req.session_id, lock=True)

    if donor.is_complete:
        raise HTTPException(status_code=400, detail="Interview already complete")

    data = {**donor.profile_data}
    state = dict(data.get("_state", _init_state()))

    # Determine current category from state
    category = state.get("current_category") if req.question_id in CATEGORY_LOOP else None

    # Store the answer
    data = _store_answer(data, req.question_id, req.answer, category)

    # Compute next question
    next_q, state = _next_question(state, req.question_id, req.answer)

    data["_state"] = state
    is_complete = next_q is None

    donor.profile_data = data
    donor.is_complete = is_complete
    save_profile(db, donor)

    return AnswerResponse(
        session_id=req.session_id,
        saved=True,
        next_question=next_q,
        complete=is_complete,
    )


@router.get(
    "/session/{session_id}",
    response_model=SessionResponse,
    summary="Get full donor session",
    description=(
        "Returns the complete donor profile including all interview answers, "
        "per-category preferences, and current state machine position. "
        "Useful for debugging or resuming a session."
    ),
    responses={404: {"description": "Session not found"}},
)
def get_session(session_id: str, db: DB):
    """Get the full donor profile and current state."""
    donor = get_or_404(db, session_id)
    return SessionResponse(
        session_id=donor.session_id,
        firstname=donor.firstname,
        is_complete=donor.is_complete,
        profile=donor.profile_data,
    )


@router.get(
    "/question/{question_id}",
    summary="Fetch a single question by ID",
    description=(
        "Returns a fully hydrated question object from the question bank. "
        "Pass an optional `category` query param for Q4a/Q4b to interpolate "
        "the `{category_name}` placeholder in display and TTS fields. "
        "Intended for frontend debugging and development."
    ),
    responses={404: {"description": "Question not found"}},
)
def get_question(question_id: str, category: str | None = None):
    """Return a single question object by ID. Useful for frontend debugging."""
    if question_id not in _load_qb():
        raise HTTPException(status_code=404, detail="Question not found")
    return _hydrate(question_id, category)
