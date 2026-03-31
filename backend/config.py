"""
Configuration — all tuneable values in one place.
Change values here; no code changes needed elsewhere.
"""
import os

# ── ElevenLabs TTS ───────────────────────────────────────────────────────────
ELEVENLABS_VOICE_ID   = os.getenv("ELEVENLABS_VOICE_ID", "qSeXEcewz7tA0Q0qk9fH")
ELEVENLABS_MODEL_ID   = os.getenv("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5")
ELEVENLABS_SPEED      = float(os.getenv("ELEVENLABS_SPEED", "1.0"))
ELEVENLABS_STABILITY  = float(os.getenv("ELEVENLABS_STABILITY", "0.65"))
ELEVENLABS_SIMILARITY = float(os.getenv("ELEVENLABS_SIMILARITY_BOOST", "0.80"))
ELEVENLABS_STYLE      = float(os.getenv("ELEVENLABS_STYLE", "0.45"))
ELEVENLABS_TTS_URL    = f"https://api.elevenlabs.io/v1/text-to-speech/{ELEVENLABS_VOICE_ID}"

# ── Models & token limits ─────────────────────────────────────────────────────
RERANKER_MODEL      = "claude-haiku-4-5-20251001"  # Stage 3 reranker — fast + cheap
RERANKER_MAX_TOKENS = 2048
PPS_MODEL           = "claude-sonnet-4-6"           # PPS generation — quality matters
PPS_MAX_TOKENS      = 512

# ── 11 donor-facing categories → mvp_charities boolean column ────────────────
CATEGORY_TO_COL = {
    "Arts, Culture & Media":                                  "cat_arts",
    "Basic Needs, Human Services & Families":                 "cat_basic_needs",
    "Economic Empowerment & Community Development & Housing": "cat_economic",
    "Education & Skill Development":                          "cat_education",
    "Environment, Climate & Animals":                         "cat_environment",
    "Equity, Justice & Civic Life":                           "cat_equity",
    "Faith & Interfaith Initiatives":                         "cat_faith",
    "International Aid & Global Development":                 "cat_international",
    "Medical Research, Health & Wellbeing":                   "cat_health",
    "Public Policy, Civic Engagement & Democracy":            "cat_policy",
    "Science, Technology & Innovation for Good":              "cat_science",
}

# ── Q6 mandatory engagement → mvp_charities boolean column ───────────────────
# "none" means no hard filter applied
MANDATORY_ENGAGEMENT_FILTER = {
    "none":               None,
    "volunteer_only":     "has_volunteer_opportunities",
    "events_only":        "has_events",
    "measurable_updates": "has_impact_metrics",
    "expertise_only":     None,  # no direct column — handled by reranker context
}

# ── Q2 values → reranker signals ─────────────────────────────────────────────
# Tells the reranker what to emphasise in explanations and ranking
Q2_RERANKER_SIGNALS = {
    "results_driven":     "Prioritise organisations with measurable impact metrics. Mention outcomes and evidence of effectiveness in your explanation.",
    "values_guided":      "Prioritise mission alignment. Emphasise how the organisation's values match the donor's stated beliefs.",
    "personal_connection":"Weight mission text match highly. Use warm, personal language in explanations.",
    "curious_exploratory":"Surface a diverse range of organisations. Mention what makes each one distinctive.",
    "strategic_impact":   "Prioritise organisations with strong financials, scale, and systems-change potential. Mention leverage and long-term outcomes.",
    "emotionally_moved":  "Lead with human stories and emotional resonance in explanations.",
    "time_constrained":   "Be concise. Return fewer, higher-confidence matches only.",
    "legacy":             "Prefer established organisations with long track records. Mention years active and financial stability.",
    "figuring_it_out":    "Offer a broad spread across the selected categories. Keep explanations accessible.",
}

# ── Donor geographic scope (Q4a) → charity geographic_scope values ───────────
GEOGRAPHY_SCOPE_MAP = {
    "Neighborhood / ZIP":                       ["City"],
    "City / Municipality":                      ["City"],
    "County / District":                        ["County", "City"],
    "Multi-County / District":                  ["County"],
    "State / Province":                         ["State", "County", "City"],
    "Regional (Multi-State / Multi-Province)":  ["Regional", "State"],
    "National (Single Country)":                ["National"],
    "Global / Worldwide":                       ["Global"],
    "Multi-Country (Regional International)":   ["Global"],
    "Chapters / Multiple Locations":            ["National", "Regional"],
    "Defined Service Area":                     ["City", "County", "State"],
    "Virtual / No Geographic Restriction":      ["City", "County", "State", "Regional", "National", "Global"],
}

# Score for how well a donor scope preference matches a charity scope value
GEOGRAPHY_SCOPE_SCORES = {
    "Neighborhood / ZIP":                       {"City": 1.0},
    "City / Municipality":                      {"City": 1.0, "County": 0.6},
    "County / District":                        {"County": 1.0, "City": 0.8},
    "Multi-County / District":                  {"County": 1.0, "Regional": 0.6},
    "State / Province":                         {"State": 1.0, "County": 0.7, "City": 0.5},
    "Regional (Multi-State / Multi-Province)":  {"Regional": 1.0, "State": 0.7, "National": 0.5},
    "National (Single Country)":                {"National": 1.0, "Regional": 0.6},
    "Global / Worldwide":                       {"Global": 1.0, "National": 0.5},
    "Multi-Country (Regional International)":   {"Global": 1.0},
    "Chapters / Multiple Locations":            {"National": 0.8, "Regional": 0.8},
    "Defined Service Area":                     {"City": 0.9, "County": 0.9, "State": 0.7},
    "Virtual / No Geographic Restriction":      {"City": 0.5, "County": 0.5, "State": 0.5, "Regional": 0.5, "National": 0.5, "Global": 0.5},
}

# ── Stage 2 scoring weights ───────────────────────────────────────────────────
# geography + population must sum to 1.0 when Q1 is empty
# when Q1 is provided, mission score is added and weights rebalanced in code
SCORE_WEIGHTS = {
    "geography":  0.40,
    "population": 0.40,
    "mission":    0.20,  # only applied when Q1 free text is provided
}

# ── Match grade thresholds (score out of 100) ─────────────────────────────────
MATCH_GRADE_THRESHOLDS = {
    "Excellent": 80,
    "Good":      60,
    "Fair":      40,
}

# ── Answer validation limits ─────────────────────────────────────────────────
MAX_CATEGORIES        = 11   # total selectable categories (Q3 list cap)
MAX_ANSWER_LIST_ITEMS = 20   # hard cap on any list-type answer
MAX_DICT_KEYS         = 20   # max keys per level in a dict answer
MAX_DICT_DEPTH        = 2    # max nesting depth for dict answers
MAX_DICT_BYTES        = 4000 # max serialized size of a dict answer

# ── Candidate pool sizes ──────────────────────────────────────────────────────
CANDIDATES_PER_CATEGORY      = 50   # Stage 2 input — top N after boolean + vector filter
RERANKER_TOP_N               = 20   # top N scored candidates sent to Haiku reranker
RERANKER_STAGE2_WEIGHT       = 0.6  # blend: 60% Stage 2 score, 40% Haiku rerank score
DEFAULT_RESULTS_PER_CATEGORY = 10   # returned to frontend if donor skips Q9

# ── IRS 990 fields to pull in matching query ──────────────────────────────────
IRS_990_FIELDS = [
    "total_revenue",
    "total_assets",
    "net_assets",
    "contributions",
    "officer_compensation",
    "other_salaries",
    "tax_year",
]

# ── Anthropic ────────────────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")

# ── TTS rate limit ────────────────────────────────────────────────────────────
TTS_RATE_LIMIT = os.getenv("TTS_RATE_LIMIT", "20/minute")

# ── Session ID validation ─────────────────────────────────────────────────────
SESSION_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"

# ── CORS ──────────────────────────────────────────────────────────────────────
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    "http://localhost:5173,http://localhost:3000,http://127.0.0.1:3000,http://[::1]:3000,null"
).split(",")
